#!/usr/bin/env python

#
#   ClustrixDB install and configuration Wizard.
#
#   Created for bug 23587.
#
#   This script is intended to do everything that a user needs to
#   go from a basic CentOS machine to a ClustrixDB Node, with
#   additional instructions on how to configure more nodes, to form
#   a cluster.

import os
import sys
import re
import glob
import readline # Automatically enhances raw_input()
import optparse
import socket
import fcntl
import struct
import subprocess
import signal
import time
import datetime
import httplib
import termios
import stat
import textwrap

CONFIG_FILE_PATH = "/etc/clustrix/clxnode.conf"
MIN_FREE_SPACE = 20 # GiB
VALID_FILESYSTEMS = ('ext4', 'xfs') # Could also just be single string
MINIMUM_OS_RAM = 1024 # MiB, left for the OS
MINIMUM_CLX_RAM = 1024*3 # MiB for clxnode: TotalMem - RESERVE_MEM - MAX_REDO
DEFAULT_PAGER = 'more'
UPSTART_PROCESS = 'clustrix'
LICENSE_FILES = ('LICENSE-SWDL', # There are a couple possible file names
                 'LICENSE-clxnode',
                 )
RPM_GLOBS = ('clustrix-common-*.x86_64.rpm',
        'clustrix-clxnode-*.x86_64.rpm',
        'clustrix-utils-*.x86_64.rpm',
        )
ALWAYS_WRITE = ('BACKEND_ADDR',
        'UI_LOGDIR',
        )
DB_INIT_TIMEOUT = 120 # Seconds to wait for clxnode to initialize
UI_INIT_TIMEOUT = 120 # Seconds to wait for WebUI to initialize
HTTP_STATUS_PATH = '/bootup/status' # From the WebUI
CLXNODE_PATH = '/opt/clustrix/bin/clxnode'

SSHD_CONFIG_PATH = '/etc/ssh/sshd_config'
SSHD_CONFIG_ATTRS = {'HostbasedAuthentication': 'yes',
        'IgnoreRhosts': 'no',
        'HostbasedUsesNameFromPacketOnly': 'yes',
        }
SSH_CLIENT_CONFIG_PATH = '/etc/ssh/ssh_config'
SSH_CLIENT_CONFIG_ATTRS = {'HostbasedAuthentication': 'yes',
        'EnableSSHKeysign': 'yes',
        }
ROOT_SHOSTS_PATH = os.path.expanduser('~root/.shosts')
ETC_HOSTS_EQUIV_PATH = '/etc/hosts.equiv'

SYSCTL_CONFIG_PATH = '/etc/sysctl.conf'
# The value for sysctl fs.aio-max-nr is the first large value that
#   made clxnode run; it may not be optimal
# All specified sysctl values greater than 1 are minimums;
#   they will not lower the system's current setting on write.
SYSCTL_CONFIG_ATTRS = {'fs.aio-max-nr': '262144',
        }
if sys.stdout.isatty():
    INITIAL_TTY_STATE = termios.tcgetattr(1) # To reset terminal on quit
else:
    INITIAL_TTY_STATE = None

def quit(signal, frame):
    """For signal.signal, to exit without stack trace"""
    print "Quitting ClustrixDB Installer..."
    # Reset TTY state, since readline tends to leave it weird
    #   when we get a ^C:
    if not INITIAL_TTY_STATE:
        # This is not a TTY, nothing to reset
        exit(0)
    termios.tcsetattr(1, termios.TCSANOW, INITIAL_TTY_STATE)
    if ConfigOption.runmode.reconfigure:
        socket_path = ConfigOption.get_var('UNIX_SOCKET_PATH').value
        http_port = ConfigOption.get_var('HTTP_PORT').value
        private_ip = ConfigOption.get_var('BACKEND_ADDR').value
        if initctl_clustrix('start', socket_path, http_port):
            print ("ClustrixDB Service restarted sucessfully.")
        else:
            print ("Error restarting ClustrixDB Service.")
    exit(0)

# Make a shorter call for ISO 8601 date/time string:
isodate = datetime.datetime.now().isoformat

def bool_to_english(b):
    m = {True: 'Yes', False: 'No'}
    if b in m: return m[b]
    return str(b) # Handles None and best default for other types

def bool_prompt(question, default=None):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be True, False, or None (meaning
        an answer is required of the user).

    The return value is True for "yes" or False for "no".

    Originally from http://code.activestate.com/recipes/577058/
    """
    valid = {"yes":True, "y":True, "ye":True,
            "no":False, "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == True:
        prompt = " [Y/n] "
    elif default == False:
        prompt = " [y/N] "
    else:
        raise ValueError("Invalid default answer: '%s'" % default)

    while True:
        msg = '\n\t'.join(textwrap.wrap(question + prompt)) + ' '
        choice = raw_input(msg).lower()
        if default is not None and choice == '':
            return default
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                            "(or 'y' or 'n').\n")

def text_prompt(question):
    """Ask for a string input, check that it is not blank, and return it."""
    while True:
        inp = raw_input("%s: " % question).strip()
        if inp: return inp

def grammar_list(items, final_delim='or'):
    """Returns a grammatically-proper list string.
    'items' elements should already be strings"""
    if len(items) == 1:
        return str(items[0])
    if len(items) == 2:
        return "%s or %s" % tuple(items)
    items = list(items) # to handle tuple inputs
    items[-1] = "%s %s" % (final_delim, items[-1])
    return ', '.join(items)

def display_license():
    """Show the license file in a pager and wait for user to exit."""
    license = ("\n\n\tClustrixDB Terms of Use not found - "
        "CONTACT CLUSTRIX SUPPORT.")
    for license_file_name in LICENSE_FILES:
        if os.path.exists(license_file_name):
            license_file = open(license_file_name)
            license = license_file.read().strip()
            license_file.close()
            break
    if 'PAGER' in os.environ:
        pager_cmd = (os.environ['PAGER'],)
    else:
        pager_cmd = (DEFAULT_PAGER,)
    pager_process = subprocess.Popen(pager_cmd, stdin=subprocess.PIPE)
    pager_process.communicate(license)
    print "-"*80
    print "\n"

def yum_install(rpm):
    """Invoke yum to install specified RPM."""
    yum = subprocess.Popen(('yum', 'install', '-y', '--nogpgcheck',
        os.path.realpath(rpm)))
    yum.communicate()
    return yum.returncode

def get_output(cmd):
    """Get stdout and stderr from a command"""
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    stdout = p.communicate()[0]
    return stdout

def have_command(cmd_name):
    """Use which to determine if a command is available on this system."""
    return get_output('which %s' % cmd_name)

def ntp_warn(warning):
    """Print a specific warning message followed by a generic one."""
    ntp_warning_msg = ("Please ensure that ntpd is installed and configured "
            "properly to connect to one or more time servers, in order to "
            "keep node clocks in sync through your cluster.")
    print " !! " * 20
    print "WARNING: %s %s" % (warning, ntp_warning_msg)
    print " !! " * 20

def initctl_clustrix(action, mysql_sock=None, http_port=None):
    """Run:
        initctl <action> clustrix
    then on 'start' action, wait for the database and the WebUI
    to become ready."""
    # Clustrix RPMs must be installed before executing
    cmd = ('initctl', action, UPSTART_PROCESS)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    status = p.communicate()[0].strip()
    if action == 'restart' and 'Unknown instance' in status:
        # Tried to restart when the process wasn't running
        # Start it now:
        return initctl_clustrix('start', mysql_sock, http_port)
    if action == 'stop':
        # Whether this succeeds or gets an 'Unknown instance', nanny is stopped
        return True
    if action == 'start' and \
            ('%s start/running' % UPSTART_PROCESS) not in status:
        # We have some kind of error
        print "Error starting Clustrix: %s" % status
        return False
    # The rest is only for use after a sucessful 'start' action:
    print ("Clustrix service started... Please wait for the database to "
        "initialize (This will take a minute.)")
    # We should have MySQLdb by now, since yum should have fetched it
    try:
        import MySQLdb
    except ImportError:
        print "Error: Python MySQLdb not found"
        return False
    db_ready = False
    t0 = time.time()
    while time.time() - t0 < DB_INIT_TIMEOUT:
        try:
            db = MySQLdb.connect(unix_socket=mysql_sock)
            c = db.cursor()
            c.execute('select 1')
            if c.fetchone()[0] == 1:
                db_ready = True
            db.close()
        except MySQLdb.OperationalError:
            # Unable to connect, wait a bit and try again
            time.sleep(1)
            sys.stdout.write('.') # Print dot to ensure we haven't disappeared
            sys.stdout.flush() # Make sure the dot shows up
        if db_ready:
            print '' # Print \n to terminate the dots
            break
    if not db_ready:
        # We exceeded the DB_INIT_TIMEOUT with no response from clxnode
        print ("Error: Database did not come up within %d "
                "seconds." % DB_INIT_TIMEOUT)
        return False
    # Database is up, so the WebUI should start migrating now
    print ("ClustrixDB initialized... Please wait for Clustrix "
        "Insight UI initialization (This will take another minute.)")
    # We'll know it's done when we can fetch a legit status JSON blob
    #   from localhost/bootup/status
    t0 = time.time()
    ui_ready = False
    while time.time() - t0 < UI_INIT_TIMEOUT:
        try:
            h = httplib.HTTPConnection('localhost:%d' % http_port) # Never fails
            h.request('GET', HTTP_STATUS_PATH) # Will raise socket.error
            r = h.getresponse()
            if not r.status == 200:
                # HTTP error, wait any try again
                time.sleep(1)
                sys.stdout.write('.') # Print dot to ensure we haven't disappeared
                sys.stdout.flush() # Make sure the dot shows up
                continue
            elif 'clustrix' in r.read():
                # We probably have a legit return blob now
                print '' # Print \n to terminate the dots
                ui_ready = True
                break
        except socket.error:
            # Could not contact HTTP server, wait and try again
            time.sleep(1)
            sys.stdout.write('.') # Print dot to ensure we haven't disappeared
            sys.stdout.flush() # Make sure the dot shows up
    if not ui_ready:
        # UI_INIT_TIMEOUT expired with no valid response from server
        print ("Error: Clustrix Insight UI did not come up within %d "
                "seconds." % UI_INIT_TIMEOUT)
        return False
    # We made it, everything is now running as expected
    return True


class ConfigFile(object):
    """Stores, Reads, and Writes clxnode.conf file"""
    def __init__(self):
        self.path = CONFIG_FILE_PATH
        # Don't need these things if we're not reading the config file:
        self.current_config = {}
        # If we see unknown options in the config file, save them here
        #   so that we can write them back out later:
        self.extra_config = {}
        if os.path.exists(self.path):
            self.load_from_file()
    def load_from_file(self):
        """Load an existing config file"""
        with open(self.path) as config_file:
            for line in config_file:
                line = line.strip()
                if '=' not in line: continue
                if line[0] == "#": continue # Comment
                k,v = [x.strip() for x in line.split('=',1)]
                self.current_config[k] = v
    def add_extra(self, option):
        """Store an unrecognized config value in self.extra_config,
        where it will be safe until we run self.write()"""
        if not option in self.current_config:
            print "ConfigFile Error: %s not found in config file" % option
            return
        self.extra_config[option] = self.current_config[option]
    def write(self, options, runmode):
        """Write a list of options to file, commenting any which are set to
        the default value."""
        dirname = os.path.dirname(self.path)
        if not os.path.exists(dirname):
            # Directory which contains config does not exist, create it
            os.makedirs(dirname)
        with open(self.path, 'w') as config_file:
            config_file.write('# ClustrixDB config file\n')
            config_file.write('# File must be valid Bash with comment, blank lines '
                                'and varible definitions only.\n\n')
            config_file.write('# Config File Generated at: %s\n' % isodate())
            if runmode.force:
                config_file.write('# This file generated with --force')
            for opt in options:
                # Write out variables in the order they're defined below
                commented = ""
                if opt.is_default() and not opt.variable_name in ALWAYS_WRITE:
                    # Write a commented version of the variable
                    # Specifically always write out certain variables,
                    #  which this script determines better than the .sh
                    commented = "#"
                config_file.write('# %s:\n' % opt.long_description)
                config_file.write('%s%s=%s\n' % (commented, opt.variable_name,
                    opt.config_string()))
            if self.extra_config:
                config_file.write('# Extra Config Variables:\n')
                for var in self.extra_options.iteritems():
                    # var is a 2 item tuple now
                    config_file.write('%s=%s\n' % var)


class RunMode(dict):
    """Store run mode flags, limit their values to True or False."""
    ordered_flags = [] # So that we may access flags in order
    def __getattr__(self, attr):
        """Allow flags which are actually dictionary elements to be accessed
        as instance attributes.
        IE: self.bar instead of self['bar']."""
        if attr in self:
            return self[attr]
        else:
            raise AttributeError # Standard response to a missing attr
    def __setattr__(self, flag_name, mode):
        """Set flag mode, raise appropriate exception if flag doesn't
        exist or mode is not either True or False."""
        if not flag_name in self:
            raise AttributeError
        if not mode in (True, False):
            raise ValueError
        self[flag_name].mode = mode
    def add_flag(self, flag_obj):
        """Add a RunFlag instance."""
        self[flag_obj.variable_name] = flag_obj
        self.ordered_flags.append(flag_obj)

class RunFlag(object):
    """Class representing a single run mode flag."""
    def __repr__(self):
        return "<RunFlag `%s`: %s>" % (self.variable_name, self.mode)
    def __nonzero__(self):
        """Use self.mode in boolean evaluations."""
        return bool(self.mode)
    def __init__(self, option_name, default, help_str, short_var=None):
        self.option_name = option_name
        # We can't have a - in the variable name:
        self.variable_name = option_name.replace('-', '_')
        self.default = default
        self.mode = default
        self.help_str = help_str
        self.short_var = short_var
        ConfigOption.runmode.add_flag(self)
    def mkoptparse(self, parser):
        """Add flag to optparse parser, potentially with a short option."""
        optparse_kwargs = {'dest':self.variable_name,
                'default':self.default,
                'action':'callback',
                'callback':self.optcallback,
                'help':self.help_str
                }
        if self.short_var:
            parser.add_option('--'+self.option_name, '-'+self.short_var,
                    **optparse_kwargs)
        else:
            parser.add_option('--'+self.option_name, **optparse_kwargs)
    def optcallback(self, option, opt_text, value, parser):
        """When this flag is specified, set mode to the non-default value."""
        self.mode = not self.default

class WizardFlag(RunFlag):
    def optcallback(self, option, opt_text, value, parser):
        """Wizard option forces wizard mode, it does not disable it."""
        self.mode = True


class ConfigOption(object):
    """Base Config Option Class.
    Most meaningful options will be subclasses of this one."""
    options = []
    option_type = "Default" # Replace in all Sub Classes
    metavar = "OPTION" # for optparse
    runmode = RunMode()
    configured = False
    loaded_from_file = False
    def __getitem__(self, key):
        """Emulate this dictionary method so we can fill in strings"""
        return self.__dict__[key]
    def __repr__(self):
        """Provide something useful to print."""
        s = "<`%s` Option:" % self.option_type
        if self.option_name: s = "%s --%s" % (s, self.option_name)
        s = "%s $%s" % (s, self.variable_name)
        if self.value != self.default: s = "%s Current Value: %s" % (s, self.value)
        s = "%s (Default: %s)>" % (s, self.default)
        return s
    def __init__(self, variable_name, description, default, per_node=False,
                 option_name=None, **kwargs):
        self.options.append(self)
        self.variable_name = variable_name # Bash variable name for config file
        self.description = description
        if not 'ClustrixDB' in description:
            # Avoid redundancy
            self.long_description = "ClustrixDB %s" % description
        else:
            self.long_description = description
        self.value = default # We'll start from here
        self.default = default # Default value
        self.per_node = per_node # Does this option apply this cluster-wide?
        # Command line argument name, if this will be configurable:
        self.option_name = option_name
        self.is_set = False
        self.extra_kwarg('extra_help', kwargs)
    @classmethod
    def get_var(self, variable):
        """Retrieve a specific option from self.options by variable_name."""
        for opt in self.options:
            if opt.variable_name == variable:
                return opt
        return None # Could not find matching variable
    def all_strings(self):
        """Print all user-facing strings, for review purposes."""
        # Probably out of date, not called during normal execution
        print "Variable Name: %s *" % self.variable_name
        print "Description: %s *" % self.description
        print "Long Description: %s" % self.long_description
        print "Default: %s *" % self.default
        if self.option_name: print "Option Name: --%s *" % self.option_name
        print "Prompt: %s" % self.prompt_str()
        print "Human Readable: %s" % self.human_value()
        print self.mkhelp()
    def extra_kwarg(self, argname, kwargs, default=None):
        """Emulate a key word argument without breaking things.
        Useful for subclasses which need unique optional arguments."""
        self.__dict__[argname] = default
        if argname in kwargs:
            self.__dict__[argname] = kwargs[argname]
    def prompt_str(self):
        """Generate string for prompt()."""
        return ("Please enter choice for %s [Default: %s]: "
                    % (self.long_description,
                        self.human_arbitrary_value(self.default)))
    def prompt(self):
        """Get input from user, pass it to set_value(), return check().

        Should be satisfactory for most Option subclasses, just overload
        prompt_str() to customize prompt message.
        This must set self.is_set=True and return self.check()"""
        user_input = raw_input(self.prompt_str()).strip()
        print
        if user_input:
            self.set_value(user_input)
        else:
            # User pressed Return with no input, use default
            self.value = self.default
            self.is_set = True
        return self.check()
    def check(self):
        """Verify current value and prompt and/or print an error if it's
        acceptable.

        Overload this in a subclass to sanity-check selected value, as
        this default version does nothing.

        Return False only on fatal errors which make it impossible to
        achieve a minimum viable configuration. Return True in all other
        cases."""
        return True
    def mkhelp(self):
        """Assemble a help string from either long_description or extra_help,
        with the latter using __getitem__() to perform string substitution
        with any instance variable as if it were a dict key."""
        if self.extra_help:
            help_str = self.extra_help % self # Using self dict emulation
            return "%s [Default: %%default]" % help_str
        return ("Use %s for %s [Default: %%default]" % (self.variable_name,
            self.long_description))
    def mkoptparse(self, parser):
        """If option_name is set, add this option to the optparse parser.
        Short options are not supported."""
        if not self.option_name:
            return # This config is not exposed to the user
        parser.add_option("--"+self.option_name, default=self.default,
                dest=self.variable_name, metavar=self.variable_name,
                type="string", action='callback', callback=self.optcallback,
                help=self.mkhelp())
    def optcallback(self, option, opt_text, value, parser):
        """Optparse callback, pass the 'value' arg to set_value() and
        ignore the rest."""
        self.set_value(value)
    def set_value(self, value):
        """Function to set self.value, so that subclasses may do some
        processing first."""
        self.value = value
        self.is_set = True
    def is_default(self):
        """See if this option has not been changed"""
        return self.value == self.default
    def mkarg(self, no_defaults=True):
        """Return an argument which may be passed to this script to set
        this option to its current value. Skipped if the option is currently
        set to its default value.

        If pre_node is true, don't print the actual value, just the name in
        brackets, for things like IP addresses which do not apply to other
        machines."""
        if not self.option_name or (self.is_default() and no_defaults):
            return None # Don't add defaults
        if self.per_node:
            # We don't want the user copying this value to other nodes
            return "--%s=<%s>" % (self.option_name, self.variable_name)
        return "--%s=%s" % (self.option_name, self.value)
    def human_arbitrary_value(self, value):
        """Convert any value to human-friendly, as far as this class
        is concerned"""
        return value
    def human_value(self):
        """Return human-friendly value, for subclasses."""
        return self.human_arbitrary_value(self.value)
    def config_string(self):
        """Return value for use in clxnode.conf file."""
        return self.value

class ConfigBoolOption(ConfigOption):
    """A class for True/False options"""
    def prompt_str(self):
        """Generate a good string for the boolean version of prompt().
        Do not need to indicate default, since bool_prompt() does that
        for us"""
        return "%s?" % self.long_description
    def prompt(self):
        """Get boolean input from user."""
        user_input = bool_prompt(self.prompt_str(), self.default)
        if user_input != None:
            self.set_value(user_input)
        else:
            # User pressed Return with no input, use default
            self.value = self.default
            self.is_set = True
        return self.check()
    def set_value(self, value):
        if value in (True, False):
            # We got a bool, just use it
            self.value = value
            self.is_set = True
            return
        # We did not get a bool, which means this is from a config file,
        #   since self.prompt() can't return a non-bool
        # The value for a bool in the config file doesn't actually matter,
        #   just whether there's something at all defined for the variable,
        #   so if we've gotten here, just toggle from default:
        self.value = not self.default
        self.is_set = True
    def human_arbitrary_value(self, value):
        """Convert bool to English."""
        return bool_to_english(value)
    def mkoptparse(self, parser):
        """If option_name is set, add this option to the optparse parser.
        Short options are not supported."""
        if not self.option_name:
            return # This config is not exposed to the user
        parser.add_option("--"+self.option_name, default=self.default,
                dest=self.variable_name, metavar=self.variable_name,
                action='callback', callback=self.optcallback,
                help=self.mkhelp())
    def optcallback(self, option, opt_text, value, parser):
        """When this flag is specified, set mode to the non-default value."""
        self.value = not self.default
    def mkarg(self, no_defaults=False):
        """This is a flag, so don't supply self.value."""
        # Ignore the no_defaults variable, it doesn't work with flags
        if self.value != self.default:
            return "--%s" % self.option_name

class ConfigPathOption(ConfigOption):
    """Option for a directory or file path."""
    option_type = "Path"
    path_variables = {}
    metavar = "PATH" # for optparse
    def __init__(self, *args, **kwargs):
        # Emulate extra kwargs without confusing things:
        self.extra_kwarg('min_free_space', kwargs) # In GiB
        # Either a single filesystem name, or sequence of them,
        #  as found in column 3 of /proc/mounts:
        self.extra_kwarg('valid_fs', kwargs)
        # We need to handle files slightly differently from dirs:
        self.extra_kwarg('is_file', kwargs, False)
        ConfigOption.__init__(self, *args, **kwargs)
        if not self.is_file:
            # Makes sense for directories only
            self.description = "%s Path" % self.description
            self.long_description = "%s Path" % self.long_description
        self.path_variables[self.variable_name] = self # For dereferencing
        self.mkdir = False # for sub-directories to check
    def get_path(self):
        """Return an absolute, dereferenced path.
        On failure to dereference, returns None, or raises RuntimeError."""
        if '$' in self.value:
            # At least one variable here
            path = self.value
            for var in re.findall("\$([a-zA-Z_]+[a-zA-Z0-9_]*)", self.value):
                if var in self.path_variables:
                    # Circular references will hit recursion limit
                    # Exception looks like:
                    #   RuntimeError: maximum recursion depth exceeded
                    try:
                        path = path.replace("$%s" % var,
                                self.path_variables[var].get_path())
                    except RuntimeError:
                        # This won't get hit because it will trip in the caller first
                        # This probably needs some work to handle smoothly.
                        print ("Error: Circular variable reference to $%s "
                                "found in $%s." % (var, self.variable_name))
                        return None
                else:
                    # Attempted to look up a variable which did not exist
                    print ("Error: Reference to $%s in $%s cannot be resolved." %
                        (var, self.variable_name))
                    return None
            return path
        else:
            # Nothing to dereference
            return self.value
    # Abbreviate some os.path functions pointed at our current path:
    def exists(self):
        return os.path.exists(self.get_path())
    def exists_dir(self):
        return os.path.isdir(self.get_path())
    def exists_file(self):
        return os.path.isfile(self.get_path())
    def exists_link(self):
        return os.path.islink(self.get_path())
    def config_string(self):
        """Return dereferenced path for use in clxnode.conf file."""
        return self.get_path()
    def human_value(self):
        """Return human-friendly value, which is just get_path() plus
        an optional warning if the current path is on a non-optimal
        filesystem."""
        if self.valid_fs:
            fs_type = self.get_fstype()
            if not fs_type == self.valid_fs and not fs_type in self.valid_fs:
                return ("%s (Recommend using %s file system instead of %s)" %
                        (self.get_path(), grammar_list(self.valid_fs),
                            self.get_fstype()))
        return self.get_path()
    def get_dir_path(self):
        """Return dirname of a file, or the full path of a directory."""
        path = self.get_path()
        if not path: return path # Pass on get_path() errors
        if not self.is_file: return path # This is already a directory
        return os.path.dirname(path)
    def prompt_str(self):
        return ("Please enter a path for %s [Default: %s]: "
                    % (self.long_description, self.get_path()))
    def ask_to_mkdir(self):
        """Prompt user to mkdir -p this path, caching choice for later use."""
        if self.is_file:
            path = os.path.dirname(self.get_path())
        else:
            path = self.get_path()
        if '$' in self.value:
            # At least one variable here
            for var in re.findall("\$([a-zA-Z_]+[a-zA-Z0-9_]*)", self.value):
                if var in self.path_variables:
                    if self.path_variables[var].mkdir:
                        # Parent dir has already gotten a 'yes' to a prompt
                        print "Creating directory: %s" % path
                        os.makedirs(path)
                        self.mkdir = True
                        return True
        if self.runmode.yes or bool_prompt("%s: %s not found, attempt "
                "to create?" % (self.long_description, path), True):
            print "Creating directory: %s" % path
            os.makedirs(path)
            self.mkdir = True # Save for later invocations
            return True
        return False
    def set_value(self, value):
        """Translate some path elements before assigning to self.value."""
        # Try to be useful with paths:
        value = os.path.expanduser(value) # Take care of ~
        # Note: won't work if a path starting with a variable is entered.
        #       That's just going to be not supported for now.
        value = os.path.abspath(value) # Must be done after expanduser()
        self.value = value
        self.is_set = True
    def get_fstype(self):
        """Determine the filesystem type for a given path."""
        # Find our mount point:
        # os.path.ismount() will take a file or dir, but avoid symlinks:
        path = os.path.realpath(self.get_path())
        while not os.path.ismount(path):
            # If this is not a mountpoint, remove the last path element
            #   and check again, until we get to /, which we assume is a
            #   mount point
            path = os.path.dirname(path)
        # /proc/mounts lines look like:
        # /dev/md0 /mnt/backup ext4 rw,relatime,barrier=1,data=ordered 0 0
        fs_type = None
        with open('/proc/mounts') as mounts_file:
            for line in mounts_file.read().split('\n'):
                line = line.strip()
                if not line: continue
                line_parts = line.split()
                if line_parts[0] == 'rootfs':
                    # This is not the actual mount record, skip it
                    continue
                if line_parts[1] == path:
                    fs_type = line_parts[2]
                    break
        return fs_type
    def check(self):
        """Verify path exists and optionally is on the right filesystem
        with sufficient free space."""
        ConfigOption.check(self) # Common option checks
        if not self.get_path():
            # Indicates get_path() could not come up with a valid path
            # No need to print an error here, get_path() will handle that
            if not self.runmode.force:
                # Get a new path
                return self.prompt()
            else:
                # Error is probably a variable issue,
                #   just write it to file and move on
                print ("Error: Unable to determine appropriate path for"
                        "%s. Cannot continue validating %s." %
                        (self.long_description,self.variable_name))
                return True # Force check to be OK
        if not self.get_path().startswith(os.sep):
            # This is not an absolute path
            # This should not happen, since set_value runs os.path.abspath()
            print "%s: %s is not an absolute path." % (self.long_description,
                    self.get_path())
            if not self.runmode.force:
                # get a new path
                return self.prompt()
            else:
                # Makes no sense to run further checks on a relative path
                print ("Error: Using relative path %s for %s: results will "
                "be unpredictable!" % (self.get_path(), self.variable_name))
                return True # Force check OK
        if not os.path.exists(self.get_dir_path()):
            # We have a directory which doesn't exist
            if not self.ask_to_mkdir():
                # User elected not to make the directory, ask for a new one:
                return self.prompt()
        if self.exists() and not self.exists_link():
            # We found _something_ other than a symlink at the right path,
            #   see what it is:
            if self.is_file:
                if self.exists_dir():
                    # Expecting a file, found a directory
                    print ("Error: Found a directory at %s instead of a file "
                            "as expected. Please choose another path." %
                            self.get_path())
                    return self.prompt()
                elif not self.runmode.yes and not bool_prompt("File exists "
                        "at %s - overwrite?" % self.get_path(), False):
                    # Found a file at this path and the user elected not to
                    # overwrite - prompt for an alternate path
                    return self.prompt()
            elif not self.is_file and not self.exists_dir():
                # Expecting a directory, found a file
                print ("Error: Found a file at %s instead of a directory "
                        "as expected. Please choose another path." %
                        self.get_path())
                return self.prompt()
        # Selected path should now exist, optionally perform extra testing now
        if self.min_free_space:
            # Verify minimum free space on volume containing path:
            # min_free_space is in GiB
            statvfs = os.statvfs(self.get_dir_path())
            # Block size in bytes * blocks available / 1GB:
            free_space = statvfs.f_frsize * statvfs.f_bavail / 1024.0**3
            if free_space < self.min_free_space:
                print ("Warning: Insufficient free space on %s - Expected at "
                        "least %d GiB." % (self.get_path(), self.min_free_space))
                if not self.runmode.force:
                    print ("Choose a new path for %s with at least %d GiB of "
                            "free space\n\tor re-run with --force to "
                            "skip this check." % (self.description,
                                self.min_free_space))
                    return self.prompt()
                else:
                    print ("Error: Insufficient free space (%.1fGiB) on %s. "
                            "Database operations will be limited. "
                            "Recommend %.1fGiB or more free space." %
                            (free_space, self.get_path(), self.min_free_space))
                    # We can continue with other checks here
            # Free space check either passed or skipped at this point
        if self.valid_fs:
            fs_type = self.get_fstype()
            if not fs_type:
                print ("Could not determine filesystem type for %s (mounted "
                        "on: %s)" % (self.get_path(), path))
            if fs_type != self.valid_fs and fs_type not in self.valid_fs:
                # valid_fs could be a single-fs string or sequence of fs types
                # the filesystem type we're on is not in that list
                print ("Warning: Filesystem type `%s` on %s is not "
                        "recommended for %s." % (fs_type, self.get_path(),
                            self.description))
                if not self.runmode.force and self.runmode.wizard:
                    # ConfigOption.configured doesn't do anything outside
                    #   of Wizard mode
                    ConfigOption.configured = bool_prompt("Accept current "
                            "settings? Enter 'No' to return to menu.", False)
                    if not ConfigOption.configured:
                        # Stop checking things now, return to main menu
                        return True
                #if not self.runmode.force:
                #    print ("Choose a new path for %s with a filesystem of type "
                #            "%s\n\tor re-run with --force to "
                #            "ignore." % (self.short_description, self.valid_fs))
                #    return self.prompt()
                #else:
                #    print ("Error: Filesystem '%s' on %s (mount point: %s) "
                #            "is not supported. Suggest using one of: %s" %
                #            (fs_type, self.get_dir_path, path, self.valid_fs))
            # If we get this far either we're running with force or
            #   we've found a satisfactory filesystem. Continue on.
        return True # If we get here, everything has passed

class IP(object):
    """Represents an IPv4 address or mask, stored as an int"""
    def __init__(self, orig_addr=None):
        if not orig_addr:
            self.addr = 0 # Represents 0.0.0.0, which is the global-listen address
            return
        try:
            self.addr = self.from_dotted(orig_addr)
        except (ValueError, AttributeError):
            try:
                self.addr = self.from_hex(orig_addr)
            except (ValueError, TypeError):
                try:
                    self.addr = int(orig_addr)
                    if self.addr < 32:
                        # Too small to be an IP, treat this as a CIDR netmask
                        tmp_addr = ['0'] * 32
                        for x in range(self.addr):
                            tmp_addr[x] = '1'
                        self.addr = int(''.join(tmp_addr), 2)
                except ValueError:
                    try:
                        assert(isinstance(orig_addr, (int, long)))
                        self.addr = orig_addr
                    except AssertionError:
                        raise ValueError('`%s` is not a known IP '
                                'address format.' % orig_addr)
    def __repr__(self):
        return "<IP %s>" % self.to_dotted()
    def __str__(self):
        return self.to_dotted()
    def __eq__(self, other):
        return self.addr == other.addr
    def __nonzero__(self):
        # 0.0.0.0 returns False
        return bool(self.addr)
    def __len__(self):
        # Define this as the number of 1 bits in an address,
        #   so it becomes a proxy for specificity of a mask
        x = 0
        a = self.addr
        while a:
            x += a&1
            a >>= 1
        return a
    def __cmp__(self, other):
        # For comparing specificity of subnet masks
        return len(self).__cmp__(len(other))
    @staticmethod
    def from_dotted(from_addr):
        """Convert from dotted octet string to int"""
        # Doesn't support octal or missing octet abbreviations
        if not from_addr.count('.') == 3:
            raise ValueError("This doesn't look like a dotted-octet address: "
                    "%s" % from_addr)
        to_addr = 0
        for octet in from_addr.split('.'):
            to_addr <<= 8
            to_addr += int(octet)
        return to_addr
    def to_dotted(self):
        """Convert from int to dotted octet string"""
        from_addr = self.addr
        to_addr = []
        for x in range(4):
            to_addr.insert(0, str(from_addr & 0xff))
            from_addr >>=8
        return '.'.join(to_addr)
    @staticmethod
    def from_hex(from_addr):
        """Convert from network-byte-order hex to int"""
        if len(from_addr) is not 8:
            # Avoid confusion with string CIDR netmasks
            raise ValueError("`%s` is too short to be an IP in hex")
        to_addr = 0
        while from_addr:
            to_addr <<= 8
            to_addr += int(from_addr[-2:], 16)
            from_addr = from_addr[:-2]
        return to_addr
    def in_subnet(self, other, mask):
        """Determine whether other_addr is in the same subnet as self.addr,
        as specified by the mask addr."""
        return self.addr & mask.addr == other.addr & mask.addr

class Interface(object):
    """Represents a single network interface"""
    def __repr__(self):
        return "<Interface %s: %s/%s>" % (self.name, self.addr, self.mask)
    def __str__(self):
        if self.mask:
            return "%s/%s" % (self.addr, self.mask)
        return str(self.addr)
    def __init__(self, name=None):
        self.name = name
        mask = None
        if name and '/' in name:
            # We've got a netmask too
            name, mask = name.split('/')
        try:
            self.addr = IP(name)
            self.get_interface() # This may end up as None, that's not a problem
        except ValueError as e:
            # name wasn't an IP formatted in any way we know
            # assume name is an interface
            self.interface = name
            self.get_addr() # Raises ValueError if interface has no IP
        if mask:
            self.mask = IP(mask)
        else:
            self.get_mask()
    def __nonzero__(self):
        """Return True if this is an external IP."""
        return (bool(self.addr) and self.name != 'lo')
    def __eq__(self, other):
        """Compare interfaces by name."""
        return self.name == other.name
    def has(self, attr):
        """Check to see if the interface name or address matches attr."""
        attr = attr.lower()
        return self.name == attr or self.addr == attr
    def get_addr(self):
        self.addr = Interfaces.ip_for_interface(self.interface)
    def get_interface(self):
        self.interface = Interfaces.interface_for_ip(self.addr)
    def get_mask(self):
        self.mask = Interfaces.mask_for_interface(self)
    def in_subnet(self, other):
        """Check to see if another address is in the same subnet as this
        interface"""
        if not all((self.mask, self.addr)):
            # If we didn't resolve an address or subnet mask for this
            # interface then nothing should be considered local
            return False
        return self.addr.in_subnet(other.addr, self.mask)

class Route(object):
    """Represents one line of /proc/net/route"""
    def __repr__(self):
        return '<Route %s/%s via %s>' % (self.destination, self.mask, self.interface_name)
    def __init__(self, line):
        self.interface_name = line[0]
        self.destination = IP(line[1])
        self.gateway = IP(line[2])
        self.mask = IP(line[7])
    def applies_to(self, other):
        """See if this route fits the given addr"""
        return self.destination.in_subnet(other, self.mask)


class Interfaces(list):
    """Represents all of the ethernet interfaces available on the machine"""
    routes = {}
    default_route = None
    ifcache = {'Global': IP()}
    def __init__(self):
        self.populate_routes()
        self.append(Interface('*'))
    @classmethod
    def populate_routes(cls):
        default = None
        net_routes = open('/proc/net/route').read().split('\n')
        net_routes.pop(0) # remove header line
        for line in net_routes:
            line = line.strip()
            if not line:
                continue
            line = line.split()
            route = Route(line)
            if route.mask == IP('00000000') and not default:
                # The first route with a /0 mask is default
                default = route.interface_name
            if not route.interface_name in cls.routes:
                cls.routes[route.interface_name] = [route]
            else:
                cls.routes[route.interface_name].append(route)
        if default:
            # Now that we have a routing table, instantiating Interface()
            #   makes more sense
            cls.default_route = Interface(default)
    @staticmethod
    def list_interface_names():
        """Enumerate the ethernet interfaces present based on /proc"""
        return [os.path.basename(x) for x in glob.glob('/sys/class/net/*')]
    @classmethod
    def list_interfaces(cls):
        """Return a list of Interface objects for interfaces present
        in the system - only if they have an address assigned."""
        iflist = [Interface()] # Start with the global interface
        for ifname in cls.list_interface_names():
            try:
                iflist.append(Interface(ifname))
            except ValueError:
                # No address assigned to this one
                continue
        return iflist
    @classmethod
    def list_addresses(cls, no_global=False):
        """Return IP objects for all non-loopback addresses present,
        optionally including the Global listen interface at 0.0.0.0"""
        addresses = []
        if not no_global:
            # Add the global-listen address
            addresses.append(IP())
        for ifname in cls.list_interface_names():
            try:
                ifaddr = cls.ip_for_interface(ifname)
            except ValueError:
                # This interface doesn't exist or doesn't have an IP
                continue
            if cls.is_loopback(ifaddr):
                continue
            addresses.append(ifaddr)
        return addresses
    @classmethod
    def available(cls, no_global=False):
        """Return a human-readable string of addresses from
        list_addresses()"""
        return ' '.join([str(addr) for addr in cls.list_addresses(no_global)])
    @classmethod
    def ip_for_interface(cls, interface):
        """Determine IPv4 address of selected interface.
        Mostly From:
        http://code.activestate.com/recipes/439094/

        Note: May raise ValueError on unknown interface or interface with no
        address assigned."""
        if interface in cls.ifcache:
            return cls.ifcache[interface]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            addr = IP(socket.inet_ntoa(fcntl.ioctl(sock.fileno(),
                0x8915,  # SIOCGIFADDR
                struct.pack('256s', interface[:15]))[20:24]))
        except IOError:
            # Requested interface does not exist or does not have address
            raise ValueError('Interface %s does not exist or has no '
                    'address assigned' % interface)
        sock.close()
        cls.ifcache[interface] = addr
        return addr
    @classmethod
    def interface_for_ip(cls, addr):
        # Don't use Interface() object because this is called in Interface.__init__
        if addr == IP():
            # This is 0.0.0.0, the Global address
            return 'Global'
        for ifname in cls.list_interface_names():
            try:
                if addr == cls.ip_for_interface(ifname):
                    return ifname
            except ValueError:
                # Attempted to get an IP for an interface that doesn't exist
                #   or doesn't have an IP assigned
                continue
        return None # This IP isn't present on any interface
    @classmethod
    def mask_for_interface(cls, interface):
        # interface is an Interface object
        if not interface.interface or interface.interface not in cls.routes:
            # No interface name or no routes for interface
            return None
        mask = None
        for route in cls.routes[interface.interface]:
            if route.applies_to(interface.addr):
                if not mask:
                    mask = route.mask
                    continue
                # If we get here the interface has at least two usable routes
                #   Pick the longest one:
                if route.mask > mask:
                    mask = route.mask
        return mask
    @classmethod
    def default_interface(cls):
        """Return the Interface object associated with the default route"""
        if not cls.default_route:
            cls.populate_routes()
        return cls.default_route
    @staticmethod
    def is_loopback(addr):
        """Check an address to see if it's in the loopback subnet"""
        lo_addr = IP('127.0.0.1')
        lo_subnet = IP(8) # Built in CIDR test
        return lo_addr.in_subnet(addr, lo_subnet)
    @classmethod
    def find_interface_in_subnet(cls, subnet):
        """Find an available interface with an address within the specified
        subnet (which is actually an Interface() object. Don't worry about it."""
        found = []
        for interface in Interfaces.list_interfaces():
            if subnet.in_subnet(interface):
                found.append(interface)
        if len(found) > 1:
            # Matched multiple interfaces, subnet isn't specific enough
            raise ValueError("Multiple interfaces match given subnet")
        elif found:
            # Matched exactly one interface, we can use it
            return found[0]
        return None

class ConfigInterfaceOption(ConfigOption):
    """Interface Option for specifying IP Addresses."""
    interfaces = {}
    option_type = "Interface"
    cluster_wide = False # IPs change between
    def __init__(self, *args, **kwargs):
        ConfigOption.__init__(self, *args, **kwargs)
        self.extra_kwarg('requires_address', kwargs, False)
        self.interfaces[self.variable_name] = self
        self.long_description = "%s Interface" % self.long_description
    def prompt_str(self):
        pstr = ("Available IP Addresses on this node: %s\n"
                % Interfaces.available(self.requires_address))
        pstr += ("Please enter an IP address to use for %s [Default: %s]: "
                    % (self.long_description, self.default))
        return pstr
    def set_value(self, value):
        """Find an Interface object which matches the user input, by either
        IP Address, interface name, or subnet (though the last two are
        undocumented)"""
        try:
            iface = Interface(value)
        except ValueError:
            # Couldn't parse this into an Interface object with an address
            print ("Error: `%s` is not a valid IP address." % value)
            return self.prompt()
        if self.runmode.force:
            # Just go with what the user supplied
            self.value = Interface(value)
            self.is_set = True
            return
        # By now we have a valid Interface object, but it might not match
        #   any actual ethernet interfaces in the system
        if not iface.interface and iface.mask:
            # We've got an address and mask with no interface name
            # Attempt to find a real interface in the same subnet
            local_iface = Interfaces.find_interface_in_subnet(iface)
            if not local_iface:
                # No interface matches, re-prompt
                print ("Error: Unable to find interface in subnet `%s`." % iface)
                return self.prompt()
            iface = local_iface
        if not iface.addr == IP() and not (iface.addr and iface.interface):
            print ("Error: `%s` is not associated with any available network "
                    "device. Please enter a valid address." % value)
            return self.prompt()
        # At this point we have a valid Interface object with address, though
        #   it may be 0.0.0.0
        if self.requires_address and iface.addr == IP():
            # We got 0.0.0.0 when a specific address is required
            print ("Error: %s requires an IP which is currently assigned to a "
                    "network interface." % self.description)
            return self.prompt()
        # If we get here, everything is valid
        self.value = iface
        self.is_set = True
    def check(self):
        """Verify that we have a usable IP for any interface which requires
        one."""
        ConfigOption.check(self)
        if self.requires_address and not self.value.addr:
            print ("Error: Valid IP address required for %s (%s)."
                    % (self.description, self.variable_name))
            return False
        return True
    def human_value(self):
        """Add description for '0.0.0.0' if necessary."""
        if not self.value.addr or self.value.addr == IP(None):
            # We have a * interface
            return "%s (Listen on all available interfaces)" % self.value
        return self.value
    def config_string(self):
        """We don't want the netmask in the config file"""
        return self.value.addr

class ConfigPortOption(ConfigOption):
    """Option for configuring a TCP and/or UDP port."""
    option_type = "Port"
    def __init__(self, *args, **kwargs):
        ConfigOption.__init__(self, *args, **kwargs)
        self.extra_kwarg('protos', kwargs, socket.SOCK_STREAM) # Default to TCP
        self.extra_kwarg('interface_name', kwargs)
        self.extra_kwarg('configurable', kwargs, True) # Some ports are fixed
        self.interface = Interface() # Unknown interface binds to *
        if self.interface_name in ConfigInterfaceOption.interfaces:
            # Dereference by interface variable_name
            self.interface = ConfigInterfaceOption.interfaces[self.interface_name].value
        self.multiproto = False
        try:
            if len(self.protos) > 1:
                self.multiproto = True
        except TypeError:
            # We didn't get a sequence of protocols, fix that
            self.protos = (self.protos,)
        self.description = "%s %s Port" % (self.description, self.proto_str())
        self.long_description = "%s Port" % self.long_description
    def proto_text(self, proto):
        """Translate socket types into human-readable names."""
        if proto == socket.SOCK_STREAM:
            return 'TCP'
        elif proto == socket.SOCK_DGRAM:
            return 'UDP'
        return 'UNKNOWN'
    def proto_str(self):
        """Make a human-readable protocol description."""
        return '/'.join([self.proto_text(x) for x in self.protos])
    def prompt_str(self):
        return ("Please enter a %s port for %s [Default: %s]: "
                    % (self.proto_str(), self.long_description, self.default))
    def set_value(self, value):
        """Convert user input to an int or None on ValueError.
        check() will look for None and do the right thing."""
        try:
            self.value = int(value)
            self.is_set = True
        except ValueError:
            # Non-integer port will not work
            self.value = None
            self.is_set = False
            print ("Error: '%s' is not a valid %s port number." % (value,
                    self.proto_str()))
    def test_port_bind(self, proto):
        """Attempt to bind a listening socket to this port

        Return tuple of:
        available: True/False
        message: description of reason port isn't available"""
        sock = socket.socket(socket.AF_INET, proto)
        try:
            sock.bind((str(self.interface.addr), self.value))
            sock.close()
            return (True, '') # No error
        except socket.error, message: # Do this v2.5-compatible
            return (False, message) # error
    def check(self):
        """Verify that we have a valid port number and that we can listen
        on it."""
        ConfigOption.check(self) # Common option checks
        if self.value > 65535 or self.value < 1:
            print("Error: %s is not a valid %s port number." % (self.value,
                self.proto_str()))
            if not self.runmode.force and self.configurable:
                return self.prompt()
            else:
                print ("Warning: Config using port %s for %s is invalid." %
                        (self.value, self.long_description))
                return True # Cannot proceed with checks, but --force
        for proto in self.protos:
            available, message = self.test_port_bind(proto)
            if not available:
                # Unable to bind to port
                print ("Error: Unable to bind to %s port %d for %s: %s" %
                        (self.proto_text(proto), self.value,
                            self.description, message))
                if not self.runmode.force:
                    if self.configurable:
                        return self.prompt()
                    else:
                        print ("Disable the service using %s port %s and "
                            "re-run this script to continue." %
                            (self.proto_text(proto), self.value))
                        return False
                # force mode can keep going on this failure
        return True # Port is satisfactory

class ConfigMemOption(ConfigOption):
    """Memory Config Option, there should never be more than one instance
    of this."""
    def __init__(self, *args, **kwargs):
        """In addition to regular init code, we also run verify that the
        machine has enough memory, otherwise we quit. It's done here so it
        will happen as early as possible."""
        ConfigOption.__init__(self, *args, **kwargs)
        self.max_redo = 1024 #MiB, Default
        for opt in self.options:
            if opt.variable_name == "MAX_REDO":
                # Look this up if available
                self.max_redo = opt.value
        self.memtotal = None
        with open('/proc/meminfo') as meminfo:
            for line in meminfo.read().split('\n'):
                line = line.strip()
                if not line or ':' not in line:
                    continue
                line = line.split()
                if line[0] == "MemTotal:":
                    self.memtotal = float(line[1])/1024
                    break
        # This is the minimum amount of memory to leave unallocated:
        self.min_reserve_ram = MINIMUM_OS_RAM + self.max_redo
        # This is the minimum amount of system memory required to run clxnode:
        self.min_sys_ram = MINIMUM_CLX_RAM + self.min_reserve_ram
        if self.memtotal < self.min_sys_ram:
            print ("Fatal Error: This system does not have enough memory "
                    "to run ClustrixDB software. Required minimum "
                    "memory is %d MiB, system has only %d MiB." %
                    (self.min_sys_ram, self.memtotal))
            exit(1)
        self.default = int(self.memtotal - self.min_reserve_ram)
        self.value = self.default
    def set_value(self, value):
        """Cast user input to int type, otherwise set value to None,
        which check() will handle properly."""
        try:
            self.value = int(value)
            self.is_set = True
        except ValueError:
            print "Error: '%s' is an invalid quantity of memory." % value
            self.value = None
    def check(self):
        """Ensure that we've got a valid value and that we still have enough
        system memory."""
        if self.memtotal < self.min_sys_ram:
            # This should have been caught during __init__, just double check
            print ("Fatal Error: This system does not have enough memory "
                    "to run ClustrixDB software. Required minimum "
                    "memory is %d MiB, system has only %d MiB." %
                    (self.min_sys_ram, self.memtotal))
            return False # This is a fatal error, even with --force
        ConfigOption.check(self)
        if not self.value:
            # We got a non-int quantity, ask again
            return self.prompt()
        if self.value < MINIMUM_CLX_RAM:
            # We did not get enough memory, possibly a negative value
            print ("Error: %.1f MiB is less than the minimum memory requirement "
                    "of %d MiB." % (self.value, MINIMUM_CLX_RAM))
            if not self.runmode.force:
                return self.prompt()
            else:
                # --force will just assume the user wants the minimum value
                self.value = MINIMUM_CLX_RAM
        if self.memtotal <  self.max_redo + MINIMUM_OS_RAM + self.value:
            print ("Error: System memory (%d MiB) is not sufficient to allocate "
                    "%d MiB to clxnode. Please enter a new value no greater than "
                    "%d MiB." % (self.memtotal, self.value, self.default))
            if not self.runmode.force:
                return self.prompt()
        return True
    def human_arbitrary_value(self, value):
        """Add units to the value."""
        return "%s MiB" % self.value


class ConfigCoresOption(ConfigOption):
    def human_arbitrary_value(self, value):
        # Make default of 0 look better:
        if self.value in (0, '0'):
            return "All"
        return self.value
    def ask_again(self, value):
        print ("Error: `%s` is an invalid number of cores." %
                value)
        return self.prompt()
    def set_value(self, value):
        # At this point value will always be a string
        if value.lower() in ('max', 'maximum', 'all'):
            ConfigOption.set_value(self, '0')
        else:
            # See if we got a number here
            try:
                if int(value) < 0:
                    return self.ask_again(value)
                else:
                    ConfigOption.set_value(self, value)
            except Exception:
                return self.ask_again(value)
    def is_default(self):
        if self.value in (0, '0', 'All'):
            return True
        return False


class SSHConfigAttr(object):
    def __init__(self, key, desired_value):
        self.key = key
        self.desired_value = desired_value
        self.current_value = None
    def is_set(self):
        return self.desired_value == self.current_value


class ConfigSSHOption(ConfigBoolOption):
    """Option to enable SSH trust configuration automatically."""
    def __init__(self, *args, **kwargs):
        ConfigOption.__init__(self, *args, **kwargs)
        self.sshd_attrs = {}
        for key, value in SSHD_CONFIG_ATTRS.iteritems():
            self.sshd_attrs[key] = SSHConfigAttr(key, value)
        self.ssh_client_attrs = {}
        for key, value in SSH_CLIENT_CONFIG_ATTRS.iteritems():
            self.ssh_client_attrs[key] = SSHConfigAttr(key, value)
    def mkhelp(self):
        # This needs to be custom for a --no argument
        help_str = self.extra_help % self # Using self dict emulation
        return "%s [Default: %s]" % (help_str, not self.default)
    def check_conf(self, name, attrs, path):
        if not os.path.exists(path):
            # Use this to determine whether sshd is installed, as well as
            #   to validate that we can open it for the following parsing.
            print ("Warning: %s configuration file not found at expected "
                    "path (%s). Cannot configure automatic ssh trust between "
                    "nodes." % (name, path))
            self.value = False
            return True
        conf = open(path).read().split('\n')
        for line in conf:
            # example lines:
            # #PrintLastLog yes
            # TCPKeepAlive yes
            # UseLogin no
            # Subsystem sftp    /usr/libexec/sftp-server
            line = line.strip()
            if not line or line.startswith('#'):
                # Blank or commented line
                continue
            try:
                key, value = line.split(None, 1)
            except ValueError:
                # Line had no whitespace to split on, ignore it
                continue
            if key in attrs:
                attrs[key].current_value = value.lower()
        for attr in attrs.values():
            if attr.current_value and attr.current_value != attr.desired_value:
                # Value is set to something else, no good
                print ("Error: %s config option %s is currently set to %s, "
                        "but it must be changed to %s to enable automatic "
                        "inter-node trust." % (name, attr.key,
                            attr.current_value, attr.desired_value))
                self.value = bool_prompt("Allow ClustrixDB to change `%s` "
                        "from `%s` to `%s` "
                        "in %s? Selecting 'No' here will disable automatic "
                        "inter-node ssh trust." %(attr.key, attr.current_value,
                            attr.desired_value, path), True)
                if not self.value: return True # Option Disabled

    def check(self):
        # Since choices made here can trample a user's system, prompt
        #   always, even if runmode.force = True
        if not self.value: return True # Option Disabled
        self.check_conf('sshd', self.sshd_attrs, SSHD_CONFIG_PATH)
        if not self.value: return True # User disabled option
        self.check_conf('ssh client', self.ssh_client_attrs,
                SSH_CLIENT_CONFIG_PATH)
        if not self.value: return True # User disabled option
        if os.path.exists(ROOT_SHOSTS_PATH):
            # We need to replace this with a symlink if it isn't already,
            #   which means we need to be careful of any potential contents
            if os.path.islink(ROOT_SHOSTS_PATH):
                root_shosts_target = os.path.realpath(ROOT_SHOSTS_PATH)
                if root_shosts_target != ETC_HOSTS_EQUIV_PATH:
                    # This is not the symlink we need
                    print ("Error: %s is currently a symlink to %s, "
                            "ClustrixDB requires it to link to %s." %
                            (ROOT_SHOSTS_PATH, root_shosts_target,
                                ETC_HOSTS_EQUIV_PATH))
                    self.value = bool_prompt("Allow ClustrixDB to replace "
                            "link from %s to %s with one to %s? Selecting "
                            "'No' here will disable automatic inter-node ssh "
                            "trust." % (ROOT_SHOSTS_PATH, root_shosts_target,
                                ETC_HOSTS_EQUIV_PATH), True)
                    if not self.value: return True # Option Disabled
            # If we make it here either we're replacing the symlink or
            #   it already points where we want it
            else:
                # ~root/.shosts is a regular file.
                shosts = open(ROOT_SHOSTS_PATH).read().strip()
                if shosts:
                    # File contains something other than whitespace
                    print ("Error: %s is not empty. ClustrixDB requires it "
                            "to be replaced with a symlink to %s." %
                            (ROOT_SHOSTS_PATH, ETC_HOSTS_EQUIV_PATH))
                    self.value = bool_prompt("Allow ClustrixDB to move "
                            "contents of %s to %s and replace %s with a "
                            "symlink to %s? Selecting 'No' here will disable "
                            "automatic inter-node ssh trust." %
                            (ROOT_SHOSTS_PATH, ETC_HOSTS_EQUIV_PATH,
                                ROOT_SHOSTS_PATH, ETC_HOSTS_EQUIV_PATH), True)
                    if not self.value: return True # Option Disabled
        # We're all good now
        return True
    def write_conf(self, attrs, path):
        if not all([attr.is_set() for attr in attrs.values()]):
            # At least one config attribute needs to be written to file
            current_conf = open(path).read().split('\n')
            new_conf = []
            for line in current_conf:
                stripped_line = line.strip()
                if not stripped_line or stripped_line.startswith('#'):
                    # Don't need to modify this, just keep it as-is
                    new_conf.append(line)
                    continue
                try:
                    key, value = stripped_line.split(None, 1)
                except ValueError:
                    # Line had no whitespace to split on, ignore it
                    new_conf.append(line)
                    continue
                if not key in attrs or attrs[key].is_set():
                    # This is either an unimportant or already-correct attr
                    new_conf.append(line)
                    continue
                # At this point the line has a conflicting value, comment it:
                new_conf.append("# Line commented by ClustrixDB Installer "
                        "at %s:" % isodate())
                new_conf.append("#%s" % line)
            for attr in attrs.values():
                if attr.is_set():
                    # Already correct in the conf file, skip
                    continue
                new_conf.append("# Added by ClustrixDB Installer at %s:"
                        % isodate())
                new_conf.append("%s %s" % (attr.key, attr.desired_value))
            # create backup of current file:
            conf_stat = os.stat(path)
            os.rename(path, "%s.bak" % path)
            conf_file = open(path, 'w')
            conf_file.write('\n'.join(new_conf) + '\n')
            conf_file.close()
            os.chmod(path, conf_stat[0]) # Apply previous mode
            # Apply previous uid and guid:
            os.chown(path, conf_stat[4], conf_stat[5])
            return True # Conf File Modified
        return False # Conf File not Modified
    def write(self):
        """Apply sshd config changes to the filesystem, as necessary."""
        if not self.value: return True # We're disabled, nothing to do
        if self.write_conf(self.sshd_attrs, SSHD_CONFIG_PATH):
            # sshd config was modified, restart sshd:
            subprocess.call('service sshd restart', shell=True)
        self.write_conf(self.ssh_client_attrs, SSH_CLIENT_CONFIG_PATH)

        # Take care of root .shosts file:
        if os.path.exists(ROOT_SHOSTS_PATH) or os.path.islink(ROOT_SHOSTS_PATH):
            # We need to replace this with a symlink if it isn't already,
            #   which means we need to be careful of any potential contents
            if os.path.islink(ROOT_SHOSTS_PATH):
                if os.path.realpath(ROOT_SHOSTS_PATH) != ETC_HOSTS_EQUIV_PATH:
                    # Unlink current symlink so we can replace it:
                    os.unlink(ROOT_SHOSTS_PATH)
                    # Create the symlink that we need:
                    os.symlink(ETC_HOSTS_EQUIV_PATH, ROOT_SHOSTS_PATH)
            else:
                # root .shosts has something in it.
                # This really isn't an equivalent solution,
                # but it's the best we can do.
                current_shosts = open(ROOT_SHOSTS_PATH).read()
                etc_hosts_equiv = open(ETC_HOSTS_EQUIV_PATH, 'a+')
                # We can't add a comment to hosts.equiv, just the data:
                etc_hosts_equiv.write(current_shosts)
                etc_hosts_equiv.close()
                # Remove root .shosts:
                os.unlink(ROOT_SHOSTS_PATH)
                # Create the symlink that we need:
                os.symlink(ETC_HOSTS_EQUIV_PATH, ROOT_SHOSTS_PATH)
        else:
            # Create the symlink that we need:
            os.symlink(ETC_HOSTS_EQUIV_PATH, ROOT_SHOSTS_PATH)
        try:
            #   If this system has SELinux, we need to restore the context
            #   of the .shosts file. If there's no SELinux support, this
            #   will fail and we'll just move on
            p = subprocess.Popen(('restorecon', ROOT_SHOSTS_PATH),
                    stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
            p.communicate()
        except OSError:
            #   This system probably lacks the restorecon executable,
            #   which means there's no SELinux, which means we can safely
            #   move on without doing anything here.
            pass

class SysctlConfig(object):
    """Class to update sysctl.conf and running sysctl settings."""
    def __init__(self, path, attrs):
        self.path = path
        self.attrs = attrs
    def write(self):
        """Modifies sysctl.conf as necessary, then runs the sysctl
        command to explicitly set each attribute on the running system."""
        current_conf = open(self.path).read().split('\n')
        new_conf = []
        remaining_attrs = self.attrs.keys()
        # Example Lines:
        #   # Useful for debugging multi-threaded applications.
        #   kernel.core_uses_pid = 1
        #   net.ipv4.tcp_tw_reuse = 1
        for line in current_conf:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('#'):
                # Don't need to modify this, just keep it as-is
                new_conf.append(line)
                continue
            try:
                key, value = [x.strip() for x in line.split('=', 1)]
            except ValueError:
                # No = to split() on, ignore line
                new_conf.append(line)
                continue
            if key not in self.attrs:
                # We don't care about this attribute
                new_conf.append(line)
                continue
            if self.attrs[key] == value:
                # This line already set correctly
                remaining_attrs.remove(key)
                new_conf.append(line)
                continue
            if self.attrs[key] == '0':
                # 0 is the only exception to the <= rule below
                # Comment out whatever non-zero value we have
                new_conf.append("# Line commented by ClustrixDB Installer "
                        "at %s:" % isodate())
                new_conf.append('# %s' % stripped_line)
                continue
            try:
                if int(self.attrs[key]) <= int(value):
                    # Current value greater than ours, keep it
                    remaining_attrs.remove(key)
                    new_conf.append(line)
                    continue
            except ValueError:
                # Something did not cast to an int, to be safe
                #   we will comment the line
                new_conf.append("# Line commented by ClustrixDB Installer "
                        "at %s:" % isodate())
                new_conf.append('# %s' % stripped_line)
            # If we've got to this point the line is a mismatch, comment it:
            new_conf.append("# Line commented by ClustrixDB Installer "
                    "at %s:" % isodate())
            new_conf.append('# %s' % stripped_line)
        # We've pulled out all conflicting values from the file, now add ours:
        for key in remaining_attrs:
            new_conf.append("# Line added by ClustrixDB Installer "
                    "at %s:" % isodate())
            new_conf.append("%s = %s" % (key, self.attrs[key]))
            # sysctl.conf only takes effect on boot, so modify each attr
            #   manually too:
            subprocess.call("sysctl %s=%s > /dev/null" % (key,
                self.attrs[key]), shell=True)
        # create backup of current file:
        conf_stat = os.stat(self.path)
        os.rename(self.path, "%s.bak" % self.path)
        conf_file = open(self.path, 'w')
        conf_file.write('\n'.join(new_conf) + '\n')
        conf_file.close()
        os.chmod(self.path, conf_stat[0]) # Apply previous mode
        # Apply previous uid and guid:
        os.chown(self.path, conf_stat[4], conf_stat[5])
        return bool(remaining_attrs) # Indicate whether we modified the file

class ConfigHugeTLBOption(ConfigBoolOption):
    """Configure option for HugeTLB, to be used by hugetlb.init.
    We want HugeTLB enabled, because it's faster, but it causes kernel
    panics on Xen PV platforms, which isn't so great.
    """
    def __init__(self, *args, **kwargs):
        """Regular init stuff, plus calculate the default value based on
        the hardware we detect."""
        ConfigOption.__init__(self, *args, **kwargs)
        self.default = True
        if os.path.exists('/sys/hypervisor/type'):
            hv_type = open('/sys/hypervisor/type').read().strip()
            if hv_type == 'xen':
                if not os.path.exists('/proc/acpi'):
                    # PV doesn't support ACPI, HVM does
                    self.default = False
            else:
                # For now we'll assume other Hypervisors don't support HugeTLB
                self.default = False
        self.value = self.default
    def check(self):
        """There's nothing really to check here, just require input if a user
        attempts to switch from a default of False."""
        if not self.runmode.yes and not self.default and self.value:
            self.value = bool_prompt("Enabling HugeTLB on this system is not "
                    "recommended, as it may cause instability. Please confirm "
                    "that you want to run an unstable configuration", False)
        return True
    def is_default(self):
        # Overload this so that ConfigFile.write() uncomments our variable
        #   when self.value is True, ignoring the default value
        return not self.value
    def set_value(self, value):
        if value in (True, False):
            # We got a bool, just use it
            self.value = value
            self.is_set = True
            return
        # We did not get a bool, which means this is from a config file,
        #   since self.prompt() can't return a non-bool
        # HugeTLB gets handled differently from other Bools - if it's set
        #   then that is treated as True
        self.value = True
        self.is_set = True
    def mkarg(self, no_defaults=False):
        # If HUGETLB is missing from the config, but it defaults to ON,
        #   then we need to add the --toggle-hugetlb flag
        if self.loaded_from_file and not self.is_set and self.default == True:
            return "--%s" % self.option_name
        if self.is_set and self.default != self.value:
            # Option was changed manually
            return "--%s" % self.option_name

class ClxnodeVersion(object):
    """Simple class to store and compare versions of clxnode."""
    def __str__(self):
        return "%s-%s" % (self.branch, self.build)
    def __eq__(self, other):
        return self.branch == other.branch and self.build == other.build
    def __init__(self, branch, build):
        self.branch = branch
        self.build = build

def get_current_clxnode():
    """Look for a clxnode binary, return version if we find it
    or None if not."""
    if not os.path.exists(CLXNODE_PATH):
        return None
    cmd="%s -version" % CLXNODE_PATH
    try:
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    except OSError:
        # Something went wrong executing clxnode
        return None
    version = p.communicate()[0].strip()
    # version looks like:
    # 5.0.45-clustrix-mainline1-9868-5e2590a9310eb112-release
    # 5.0.45-clustrix-v5.1-9868-5e2590a9310eb112-release
    # 5.0.45-clustrix-6.0.1 # This is a transition release string
    # clustrix-6.0.1 # This is the new release string
    try:
        # Parse out branch and build:
        branch, build = version.split('-')[2:4]
        return ClxnodeVersion(branch, build)
    except:
        # Got a new-style release string
        release = version.split('clustrix-')[-1]
        return ClxnodeVersion(release, 0) # Release RPMs have 0 for the build field

def get_included_clxnode():
    """Check the clxnode RPM packaged with this script for version number."""
    # First, find appropriate glob:
    for clxnode_glob in RPM_GLOBS:
        if 'clxnode' in clxnode_glob:
            break
    # glob will now match the clxnode RPM:
    clxnode_rpm = glob.glob(clxnode_glob)
    if not clxnode_rpm:
        # No RPM found
        return None
    clxnode_rpm = clxnode_rpm[0] # Pick the first RPM we find
    # File name looks like:
    # clustrix-clxnode-purelicense-mainline1-9868.x86_64.rpm
    # clustrix-clxnode-purelicense-v5.1-9868.x86_64.rpm
    # clustrix-clxnode-purelicense-6.0.1-0.x86_64.rpm
    # Parse out branch and build:
    branch, build = clxnode_rpm.rsplit('.',2)[0].rsplit('-',2)[1:3]
    return ClxnodeVersion(branch, build)


# Flags before Options:
WizardFlag('wizard', True, short_var='w', help_str="Run in Wizard mode to "
        "prompt user for all variables not specified on the command "
        "line. This is the default if no arguments are given (other than "
        "--reconfigure.)")
RunFlag('yes', False, short_var='y', help_str="Assume 'yes' to all yes/no "
    "prompts. Disables Wizard mode unless --wizard is specified explicitly.")
RunFlag('force', False, short_var='f', help_str="Skip validation checks on "
    "all options. Disables Wizard mode unless --wizard is specified "
    "explicitly. Implies --yes. "
    "WARNING: May not write a viable config file.")
RunFlag('load-config', False, "Read the ClustrixDB config file (%s) to "
    "populate options from current settings" % CONFIG_FILE_PATH)
RunFlag('skip-rpms', False, "Do not automatically install ClustrixDB "
    "RPMs located in the current directory. Implies --no-autorun.")
RunFlag('reconfigure', False, "Change configuration options on existing "
        "ClustrixDB Installation. Implies --load-config")
RunFlag('no-autorun', False, "Do not automatically start ClustrixDB "
        "service after installation.")
RunFlag('print-config', False, "Print the command required to configure "
        "another node to join this cluster and exit, without modifying the "
        "current running system.")


# Config options are global
ConfigMemOption("NODE_MEMORY", "Memory to use for ClustrixDB, in MiB",
        1024, option_name="clxnode-mem", extra_help="Use %(variable_name)s "
        "to specify how much memory (in MiB) to allocate for "
        "ClustrixDB.")
ConfigOption("MAX_REDO", "Maximum ClustrixDB Redo Space, in MiB", 1024)
ConfigCoresOption("CPU_CORES", "CPU cores to use for ClustrixDB",
        'All', option_name="cpu-cores", extra_help="Use %(variable_name)s "
        "to limit the number of CPU cores used by ClustrixDB. Set equal "
        "to or less than the licensed core count.")
#ConfigOption("CLUSTER_NAME", "Cluster Name", "clx", option_name="cluster-name")
# Path globals must stay in the correct order, for references to work
ConfigPathOption("DATA_PATH", "Database Storage", "/data/clustrix",
        option_name="data-path", min_free_space=MIN_FREE_SPACE,
        valid_fs=VALID_FILESYSTEMS)
ConfigPathOption("LOG_PATH", "Logs", "$DATA_PATH/log", option_name="log-path")
ConfigPathOption("UI_LOGDIR", "WebUI Logs", "$LOG_PATH/clustrix_ui",
        option_name="ui-log-path")
ConfigPathOption("UI_CACHEDIR", "WebUI Cache", "/var/cache/clustrix/django")
ConfigPathOption("UNIX_SOCKET_PATH", "MySQL Protocol Unix Socket",
        "/var/lib/mysql/mysql.sock", option_name="unix-socket", is_file=True)

# Interface must come before dependent port(s):
ConfigInterfaceOption("LISTEN_ADDR", "Database Listen Address (Front-End IP)",
        Interface(), option_name='listen-addr',
        extra_help="Use %(variable_name)s to specify on which IP addresses "
        "ClustrixDB will accept client connections. This must be "
        "an IP assigned to this host, or 0.0.0.0 to allow connections at "
        "any IP address assigned to this host.")
ConfigPortOption("MYSQL_PORT", "Database MySQL", 3306,
        interface_name="FRONTEND_ADDR", option_name="mysql-port",
        extra_help="Use %(variable_name)s to specify the TCP port on which "
        "ClustrixDB will accept MySQL client connections")
ConfigInterfaceOption("BACKEND_ADDR", "Private (Back-End) IP",
        Interfaces.default_interface(), option_name='cluster-addr',
        requires_address=True)
ConfigPortOption("BACKEND_PORT", "Back End Network", 24378,
        protos=(socket.SOCK_STREAM, socket.SOCK_DGRAM),
        interface_name="BACKEND_ADDR", option_name="cluster-port")
ConfigPortOption("HTTP_PORT", "WebUI HTTP", 80, option_name="http-port")
ConfigPortOption("NANNY_PORT", "Nanny", 2424,
        configurable=False)
ConfigPortOption("CONTROL_PORT", "Control", 2048,
        configurable=False)

ConfigSSHOption("WRITE_HOSTS", "Allow ClustrixDB to modify sshd_config and "
        "/etc/hosts. This is required for internode communication for "
        "administrative tasks, including upgrades", True,
        option_name="no-configure-sshd-trust",
        extra_help="Do not allow ClustrixDB to enable Host-Based "
        "Authentication in /etc/ssh/sshd_config and /etc/ssh/ssh_config "
        "or modify /etc/hosts.")

ConfigHugeTLBOption("HUGE_TLB_ENABLE", "Enable HugeTLB memory allocation "
        "for faster startup. NOTE: This causes instability on some systems, "
        "contact Clustrix Support before changing from default", None,
        option_name="toggle-hugetlb", extra_help="Use --%(option_name)s to "
        "toggle HugeTLB memory allocation in ClustrixDB. Please check with "
        "Clustrix Support before modifying this value from the default, "
        "especially in virtualized environments.")


def main():
    runmode = ConfigOption.runmode
    configfile = ConfigFile()
    if len(sys.argv) > 1 or not sys.stdout.isatty():
        # Wizard mode only implicit with no args:
        runmode.wizard = False
    # Build and Run optparse here
    parser = optparse.OptionParser()
    for flag in runmode.ordered_flags:
        flag.mkoptparse(parser)
    for opt in ConfigOption.options:
        # Add each user-frobbable option to parser:
        opt.mkoptparse(parser)

    (options, args) = parser.parse_args()

    if runmode.load_config or runmode.print_config:
        # Read config file and apply any settings we find:
        # Do this early so that print_config can exit before anything happens
        for file_opt in configfile.current_config:
            opt = ConfigOption.get_var(file_opt)
            if not opt:
                # We have a value set in the file which isn't handled
                #   by this script. Store it for later:
                configfile.add_extra(opt)
                continue
            opt.set_value(configfile.current_config[file_opt]) # Store Value
        ConfigOption.loaded_from_file = True
    if runmode.print_config:
        # Print the arg string required to configure another node and exit
        print ' '.join([x.mkarg(False) for x in ConfigOption.options if x.mkarg(False)])
        exit(0)

    # Ensure we're running as root:
    # Don't run this earlier, otherwise we can't print --help as non-root
    if os.geteuid() != 0:
        print "Error: root privileges required to run."
        print "Please execute %s as root." % sys.argv[0]
        exit(1)

    current_clxnode = get_current_clxnode()
    included_clxnode = get_included_clxnode()
    if current_clxnode and included_clxnode:
        if not included_clxnode == current_clxnode:
            # Different versions, customer may be trying to use an install
            #   package to upgrade, which will not work
            print ("Packaged ClustrixDB version (%s) does not match installed "
                    "version (%s.)" % (included_clxnode, current_clxnode))
            print ("If you would like to upgrade your installed version, "
                    "contact Clustrix Support. Otherwise you may reconfigure "
                    "your installed software.") # XXX needs work
        else:
            # Same version, subsequent run
            print "Clustrix version %s already installed." % current_clxnode
    if current_clxnode and not runmode.reconfigure:
        # Prompt to reconfigure or quit now:
        reconfig = bool_prompt("\nReconfigure ClustrixDB? Enter Y to "
                "reconfigure, N to quit. Reconfiguration will briefly stop "
                "ClustrixDB service to allow changes to take effect.", True)
        if not reconfig: exit() # Nothing to do
        runmode.reconfigure = True
    if runmode.force:
        # This implies --yes
        runmode.yes = True
    if runmode.reconfigure:
        # This option doesn't work if we don't have clxnode installed:
        if not current_clxnode:
            runmode.reconfigure = False
        else:
            # This implies --load-config and --skip-rpms
            runmode.load_config = True
            runmode.skip_rpms = True
            if not runmode.wizard and len(sys.argv) == 2:
                # --reconfigure was the only option, enable wizard mode
                runmode.wizard = True


    if runmode.reconfigure:
        # We don't want clxnode tieing up the various ports when we check them
        #   to make sure they're available.
        initctl_clustrix('stop')
    # Iterate through options to get user input and validate:
    if runmode.wizard:
        while not ConfigOption.configured:
            print "Starting ClustrixDB Install Wizard...\n"
            done = False
            user_options = [ x for x in ConfigOption.options if x.option_name]
            while not done:
                for x, opt in enumerate(user_options):
                    # Print current config, with ID numbers for selection:
                    print "%2d - %s: %s" % (x, opt.description,
                            opt.human_value())
                print "%2d - Display ClustrixDB Terms of Use" % (x+1)
                print " Q - Quit"
                print " H - Help"
                while True:
                    # prompt until we get valid input
                    user_input = text_prompt("\nSelect item to change or enter "
                            "'Yes' to accept Terms of Use and continue").strip()
                    try:
                        selection = int(user_input)
                    except ValueError:
                        # input was not an integer, so maybe it was 'Yes'
                        if user_input.lower() in ('y', 'yes'):
                            # User wants to accept values and continue
                            done = True
                            break
                        elif user_input.lower() in ('q', 'quit'):
                            # Exit now
                            print "Quitting ClustrixDB Installer..."
                            if runmode.reconfigure:
                                socket_path = ConfigOption.get_var('UNIX_SOCKET_PATH').value
                                http_port = ConfigOption.get_var('HTTP_PORT').value
                                private_ip = ConfigOption.get_var('BACKEND_ADDR').value
                                if initctl_clustrix('start', socket_path, http_port):
                                    print ("ClustrixDB Service restarted sucessfully.")
                                else:
                                    print ("Error restarting ClustrixDB Service.")
                            exit(0)
                        elif user_input.lower() in ('h', 'help'):
                            # Print help text for each option
                            for (n, opt) in enumerate(user_options):
                                help_str = opt.mkhelp().replace('%default',
                                        str(opt.default))
                                print ("=== %d - %s: === \n\t%s\n" % (n,
                                    opt.description, help_str))
                            raw_input("Press Enter to return to main menu: ")
                            print '\n'
                            break # go back to main menu
                        else:
                            # Neither an integer, 'Yes,' or 'Q' - prompt again
                            continue
                    # We've got an integer now
                    if selection > len(user_options) or selection < 0:
                        # Input integer out of bounds
                        print "Error: %d not understood" % selection
                        continue # go back to prompt
                    if selection == len(user_options):
                        # This is the license, since the other options start at 0
                        display_license()
                        break # Re-print config list and original prompt
                    # Now we have an actual config option selected, go for a prompt:
                    user_options[selection].prompt() # Use return value?
                    break # Re-print the config list and original prompt
            ConfigOption.configured = True # Until a later .check() sets it to false
            for opt in ConfigOption.options:
                if not opt.check():
                    # The check() method returns False if it is unable to make a
                    #   minimally-functional configuration choice.
                    # This will almost never happen - either the user will
                    #   keep entering invalid information,
                    #   or opt.check() will return True
                    print ("Unable to achieve a minimum valid config. Contact "
                            "Clustrix Support for assistance.")
                    exit(1)
                if not ConfigOption.configured:
                    # Some .check() wants us to go back to the main menu
                    break
    else: # not runmode.wizard
        # We still need to run the check() loop here
        for opt in ConfigOption.options:
            if not opt.check():
                print ("Unable to achieve a minimum valid config. Contact "
                        "Clustrix Support for assistance.")
                exit(1)
    print "\nClustrixDB successfully configured!"
    # Write config file:
    configfile.write(ConfigOption.options, runmode)
    # Possibly configure ssh:
    sshd_option = ConfigOption.get_var("WRITE_HOSTS")
    if sshd_option.value:
        sshd_option.write()
    # Set up sysctl:
    sysctl = SysctlConfig(SYSCTL_CONFIG_PATH, SYSCTL_CONFIG_ATTRS)
    sysctl.write()
    # Attempt to install RPMs
    if not runmode.skip_rpms:
        # Install RPMs
        rpm_list = []
        for rpmglob in RPM_GLOBS:
            rpm_list.append(glob.glob(rpmglob))
        if len([x for x in rpm_list if x]) == len(RPM_GLOBS):
            # We found every RPM we were looking for, keep trying to install
            for rpms in rpm_list:
                # rpms is a list from glob, with ideally just one element
                rc = yum_install(rpms[0])
                if rc:
                    print "Error installing %s" % rpms[0]
                    if not runmode.no_autorun:
                        print "ClustrixDB service has not been started."
                    break
            if not rc:
                # We didn't fail!
                print "\nClustrixDB RPMs installed successfully"
                if not runmode.no_autorun:
                    socket_path = ConfigOption.get_var('UNIX_SOCKET_PATH').value
                    http_port = ConfigOption.get_var('HTTP_PORT').value
                    private_ip = ConfigOption.get_var('BACKEND_ADDR').value.addr
                    if http_port != '80':
                        url = "http://%s:%s/" % (private_ip, http_port)
                    else:
                        url = "http://%s/" % private_ip
                    if initctl_clustrix('start', socket_path, http_port):
                        print "\nClustrixDB is now ready for use."
                        print ("\nOpen %s in a web browser if this "
                        "is the first or only host in your cluster.\nAdd %s "
                        "to the list of IP addresses in the 'Nodes to Add' "
                        "dialog if you are adding this node to a cluster." %
                        (url, private_ip))
                    else:
                        print "Error: Could not start ClustrixDB Service"
                        print "\tContact Clustrix Support for assistance."
                else:
                    # Let the user know how to start the service
                    print "Start Clustrix with 'initctl start clustrix' as root"
        elif not runmode.skip_rpms:
            # RPM install requested but no RPMs found
            print "\nNo ClustrixDB RPMs found - install them manually and run:"
            print "\tinitctl start clustrix"
            print "to start the ClustrixDB Service.\n"
    elif runmode.reconfigure:
        # Upon reconfiguration, restart initctl clustrix job:
        socket_path = ConfigOption.get_var('UNIX_SOCKET_PATH').value
        http_port = ConfigOption.get_var('HTTP_PORT').value
        private_ip = ConfigOption.get_var('BACKEND_ADDR').value.addr
        if http_port not in ('80', 80): # Match either type
            url = "http://%s:%s/" % (private_ip, http_port)
        else:
            # Don't add the port when its default
            url = "http://%s/" % private_ip
        if initctl_clustrix('start', socket_path, http_port):
            # Restarted or Started OK
            print ("ClustrixDB Service restarted sucessfully. If your cluster "
                    "has previously been configured, you may continue to use "
                    "it, otherwise, if this is your first or only node, open "
                    "%s in a web browser and continue to configure your "
                    "cluster. If you are adding this node to an existing "
                    "cluster, enter '%s' to the list of IP addresses in the "
                    "'Nodes to Add' dialog." % (url, private_ip))

    # Now that the RPMs are installed, ntp should be available and running
    # Do some sanity checks and warn on ungood conditions:
    if not have_command('ntpq'):
        # This comes in the ntp package, which should have just been installed
        ntp_warn("ntp not found.")
    else:
        # Note: this command may take ~20 seconds to complete:
        ntp_peers = get_output('ntpq -p').strip()
        if 'Connection refused' in ntp_peers:
            # Could not connect to ntpd, attempt to start service
            print 'NOTE: ntpd is not running - attempting to start service.'
            p = subprocess.Popen('service ntpd start'.split())
            p.wait()
            time.sleep(5) # Give it some time to initialize
            ntp_peers = get_output('ntpq -p').strip()
        if 'Connection refused' in ntp_peers:
            # Could not connect to ntpd again
            ntp_warn('ntpd is not running.')
        else:
            # Got some peers info back from ntpd
            # Ex:
            #       remote           refid      st t when poll reach   delay   offset  jitter
            #   ==============================================================================
            #    LOCAL(0)        .LOCL.          10 l   6h   64    0    0.000    0.000   0.000
            #   *midget.colo.spr 38.229.71.1      3 u  834 1024  377    2.048   -0.121  11.800
            # Example with no servers configured:
            #   No association ID's returned
            # Column info: http://www.eecis.udel.edu/~mills/ntp/html/ntpq.html - peers section
            try:
                ntp_peers = ntp_peers.split('\n')[2:] # Ignore first two lines
                for peer in ntp_peers:
                    if peer.split()[3] == 'l':
                        # This is the local clock - does not provide sync
                        continue
                    if peer[0].lower() in ('x', '.', '-'):
                        # The leading character indicates peer status, as per
                        #   the 'T' column of the 'Select Field' table here:
                        # http://www.eecis.udel.edu/~mills/ntp/html/decode.html#peer
                        # A leading space is a bad status, but that's what
                        # all servers start with before ntpd has had time to
                        # get sync'd up, so we'll allow it.
                        continue
                    # If we get this far, we probably have a legit time source
                    print "Note: ntpd is running with acceptable configuration."
                    break
                else:
                    # We got through the whole peers output with finding a
                    #   proper time server
                    ntp_warn("No valid NTP time source found.")
            except:
                ntp_warn('NTP Peers parse failure.')

    if not ConfigOption.runmode.reconfigure:
        # Print config command for other nodes for new installs:
        print '' # newline
        print "= "*39 # Wide dotted bar
        arg_string = ' '.join([x.mkarg() for x in ConfigOption.options if x.mkarg()])
        if 'CLXSRC' in os.environ:
            # We got this from the web, print an automatic download / install command:
            version = os.environ['VERSION']
            clxsrc = os.environ['CLXSRC']

            cmd = ['export CLXSRC="%s"' % clxsrc,
                    'export VERSION="%s"' % version,
                    'curl -s %s/%s.tar.bz2 | tar xvj' % (clxsrc, version),
                    'cd %s' % version,
                    "sudo -E %s %s --yes" % (sys.argv[0], arg_string)
                    ]
            print ("Run this command on other machines to configure them as "
                    "additional ClustrixDB nodes:")
            print '; '.join(cmd)
        else:
            print ("Run this command on other machines (after untarring) to "
                "configure them as additional ClustrixDB nodes:")
            print "\t%s %s --yes" % (sys.argv[0], arg_string)
        print "= " * 39
    print ("\n*** This Node's IP (Needed later during cluster configuration): %s" %
            ConfigOption.get_var('BACKEND_ADDR').value.addr)



if __name__ == "__main__":
    signal.signal(signal.SIGINT, quit)
    # So we can have an interactive wizard with curl | sh:
    if sys.stdout.isatty():
        sys.stdin = open('/dev/tty',  'r', os.O_NOCTTY)
    elif not '--print-config' in sys.argv:
        print >> sys.stderr, ("Running with no TTY - if command hangs, it's "
                "probably waiting for input before flushing stdout. Try "
                "again with a TTY, or use --force to work around this issue.")
        sys.stdout.flush()
    main()

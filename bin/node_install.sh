

#!/bin/bash
yum complete-transaction
yum complete-transaction
yum complete-transaction
yum –y update
yum -y install wget screen ntp ntpdate bzip bzip2 vim openssh-clients
#mkdir -p /data/clustrix/log
#mkfs.ext4 /dev/sdd
#mkfs.ext4 /dev/sde
#mount /dev/sdd /data/clustrix
#mount /dev/sde /data/clustrix/log
#echo "/dev/sdd /data/clustrix ext4 defaults,noatime,nodiratime 0 2" >> /etc/fstab
#echo "/dev/sde /data/clustrix/log ext4 defaults,noatime,nodiratime 0 2" >> /etc/fstab
chkconfig ntpd on
ntpdate pool.ntp.org
/etc/init.d/ntpd start
chkconfig iptables off
/etc/init.d/iptables stop
./clxnode_install.py -y --cluster-addr={{ private_IP }}

#!/bin/bash

CLUSTER_NAME="{{cluster_name}}"
NODE_IPS="{{IP_list}}"
CLUSTRIX_LICENSE="{{license_key}}"
DB_PWD="{{ db_root_pwd }}"

    mysql -e "SET PASSWORD FOR 'root'@'%' = PASSWORD(\"$DB_PWD\")"
    mysql -e "set global license = $CLUSTRIX_LICENSE"
    mysql -e "INSERT INTO clustrix_ui.clustrix_ui_systemproperty (name) VALUES (\"install_wizard_completed\")"
    mysql -e "set global cluster_name = \"$CLUSTER_NAME\""
 
    #add nodes by ip to cluster: 
    for i in $NODE_IPS; do
        mysql -e "alter cluster add \"$i\""
        sleep 5
    done
    #log "Completed cluster setup on ${HOSTNAME}"    

exit 0

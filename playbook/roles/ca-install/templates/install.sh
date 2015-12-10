#!/bin/bash

DATABASE_ADMIN_USERNAME="{{ db_admin_username }}"
DATABASE_ADMIN_PASSWORD="{{ db_admin_password }}"
UIM_ADMINISTRATOR_USER_NAME="{{ uim_admin_username }}"
UIM_ADMINISTRATOR_USER_PASSWORD="{{ uim_admin_password }}"
yum-complete-transaction -y
yum-complete-transaction -y
yum-complete-transaction -y

cd /uim

PW='{{ fileshare_password }}'
mount -t cifs -o domain='t3n',username='svc_ecosys',password=`echo $PW` //172.17.1.23/Software/Ecosystem_Images /mnt

cp /mnt/CA/UIM/uim.zip .
umount /mnt
unset PW

unzip -o uim.zip

perl -p -i -e "s/DB_ADMIN_USER=root/DB_ADMIN_USER={{ db_admin_username }}/ig" NMS_installer.properties
perl -p -i -e "s/DB_ADMIN_PASSWORD=nimsoft/DB_ADMIN_PASSWORD={{ db_admin_password }}/ig" NMS_installer.properties
perl -p -i -e "s/NIM_USER=administrator/NIM_USER={{ uim_admin_username }}/ig" NMS_installer.properties
perl -p -i -e "s/NMS_PASSWORD=nimsoft/NMS_PASSWORD={{ uim_admin_password }}/ig" NMS_installer.properties
perl -p -i -e "s/NIMBUS_USERNAME=administrator/NIMBUS_USERNAME_USER={{ uim_admin_username }}/ig" UMP_installer.properties
perl -p -i -e "s/NIMBUS_PASSWORD=nimsoft/NIMBUS_PASSWORD={{ uim_admin_password }}/ig" UMP_installer.properties
perl -p -i -e "s/nimsoft/{{ db_admin_password }}/ig" mysql-init.sql

./provision.sh {{ db_admin_password }}

exit 0


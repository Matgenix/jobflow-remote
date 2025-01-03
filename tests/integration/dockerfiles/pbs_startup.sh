#!/bin/bash

# Determine whether script is running as root
sudo_cmd=""
if [ "$(id -u)" != "0" ]; then
    sudo_cmd="sudo"
    sudo -k
fi

sed -i -e "s/PBS_SERVER=.*/PBS_SERVER=$(hostname)/" -e "s/PBS_START_MOM=0/PBS_START_MOM=1/" /etc/pbs.conf
# make sure that the $clienthost is present and set to the correct host
grep -q "^\$clienthost " /var/spool/pbs/mom_priv/config || echo "\$clienthost $(hostname)" >> /var/spool/pbs/mom_priv/config
sed -i "s/\$clienthost .*/\$clienthost $(hostname)/" /var/spool/pbs/mom_priv/config
LANG=C /etc/init.d/pbs start

#enable history
/opt/pbs/bin/qmgr -c "set server job_history_enable=True"

service ssh start

# Revoke sudo permissions
if [[ ${sudo_cmd} ]]; then
    sudo -k
fi

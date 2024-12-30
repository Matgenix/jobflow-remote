#!/bin/bash
# Startup script for Slurm container, vendored from https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/startup.sh

# Determine whether script is running as root
sudo_cmd=""
if [ "$(id -u)" != "0" ]; then
    sudo_cmd="sudo"
    sudo -k
fi

sed -i -e "s/PBS_SERVER=.*/PBS_SERVER=$(hostname)/" -e "s/PBS_START_MOM=0/PBS_START_MOM=1/" /etc/pbs.conf
sed -i "s/\$clienthost .*/\$clienthost $(hostname)/" /var/spool/pbs/mom_priv/config
LANG=C /etc/init.d/pbs start

#enable history
/opt/pbs/bin/qmgr -c "set server job_history_enable=True"

service ssh start

# Revoke sudo permissions
if [[ ${sudo_cmd} ]]; then
    sudo -k
fi

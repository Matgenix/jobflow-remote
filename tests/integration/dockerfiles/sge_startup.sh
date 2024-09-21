#!/bin/bash
# Startup script for Slurm container, vendored from https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/startup.sh

# Determine whether script is running as root
sudo_cmd=""
if [ "$(id -u)" != "0" ]; then
    sudo_cmd="sudo"
    sudo -k
fi

# Run the SGE installation scripts at startup as the docker network is not initialised during build
${sudo_cmd} bash <<SCRIPT
cd /opt/sge && yes "" | ./install_qmaster
source /opt/sge/default/common/settings.sh
/opt/sge/default/common/sgemaster start
cd /opt/sge && yes "" | ./install_execd
service ssh start
SCRIPT

# Revoke sudo permissions
if [[ ${sudo_cmd} ]]; then
    sudo -k
fi

source /opt/sge/default/common/settings.sh

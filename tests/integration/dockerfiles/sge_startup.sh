#!/bin/bash
# Startup script for Slurm container, vendored from https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/startup.sh

# Determine whether script is running as root
sudo_cmd=""
if [ "$(id -u)" != "0" ]; then
    sudo_cmd="sudo"
    sudo -k
fi

# Run the SGE installation scripts at startup as the docker network is not initialised during build
# Both install scripts will report errors, as systemd is not present and will fail to start them,
# so we redirect the output to avoid confusion and manually launch the services
${sudo_cmd} bash <<SCRIPT
cd /opt/sge && yes "" | ./install_qmaster 2>/dev/null
/opt/sge/default/common/sgemaster start
cd /opt/sge && yes "" | ./install_execd 2>/dev/null
/opt/sge/default/common/sgeexecd start
source /opt/sge/default/common/settings.sh
qconf -as $HOSTNAME
service ssh start
SCRIPT

# Revoke sudo permissions
if [[ ${sudo_cmd} ]]; then
    sudo -k
fi

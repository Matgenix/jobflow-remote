#!/bin/bash

# Determine whether script is running as root
sudo_cmd=""
if [ "$(id -u)" != "0" ]; then
    sudo_cmd="sudo"
    sudo -k
fi

service ssh start

# Revoke sudo permissions
if [[ ${sudo_cmd} ]]; then
    sudo -k
fi

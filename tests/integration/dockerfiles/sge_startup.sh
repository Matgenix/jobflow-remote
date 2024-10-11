#!/bin/bash
# Startup script for SGE container, modified from https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/startup.sh
set -e

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

# SGE user access configuration
# Create host group configuration
echo "group_name @allhosts
hostlist $HOSTNAME" > allhosts.txt

# Create user configuration
echo "name         jobflow
oticket      0
fshare       0
delete_time 1728730547
default_project NONE" > jobflow_user.txt

# Add user
qconf -Auser jobflow_user.txt

# Add user to default user list
qconf -au jobflow default

# Add host group
qconf -Ahgrp allhosts.txt

# Create a default queue configuration
echo "qname                 all.q
hostlist              @allhosts
seq_no                0
load_thresholds       np_load_avg=1.75
suspend_thresholds    NONE
nsuspend              1
suspend_interval      00:05:00
priority              0
min_cpu_interval      00:05:00
processors            UNDEFINED
qtype                 BATCH INTERACTIVE
ckpt_list             NONE
pe_list               make smp mpi
rerun                 FALSE
slots                 4
tmpdir                /tmp
shell                 /bin/sh
prolog                NONE
epilog                NONE
shell_start_mode      posix_compliant
starter_method        NONE
suspend_method        NONE
resume_method         NONE
terminate_method      NONE
notify                00:00:60
owner_list            jobflow
user_lists            default
xuser_lists           NONE
subordinate_list      NONE
complex_values        NONE
projects              NONE
xprojects             NONE
calendar              NONE
initial_state         default
mem_limit             INFINITY
s_rt                  INFINITY
h_rt                  INFINITY
s_cpu                 INFINITY
h_cpu                 INFINITY
s_fsize               INFINITY
h_fsize               INFINITY
s_data                INFINITY
h_data                INFINITY
s_stack               INFINITY
h_stack               INFINITY
s_core                INFINITY
h_core                INFINITY
s_rss                 INFINITY
h_rss                 INFINITY
s_vmem                INFINITY
h_vmem                INFINITY" > all.q.txt

# Add queue
qconf -Aq all.q.txt

# Register hostname as a submit host
qconf -as $HOSTNAME

service ssh start
SCRIPT

# Revoke sudo permissions
if [[ ${sudo_cmd} ]]; then
    sudo -k
fi

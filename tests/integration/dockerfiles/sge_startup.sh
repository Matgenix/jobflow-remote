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

# Create global scheduler configuration file
echo "algorithm                         default
schedule_interval                   0:0:1
maxujobs                           0
queue_sort_method                  load
job_load_adjustments              NONE
load_adjustment_decay_time         0:7:30
load_formula                      np_load_avg
schedd_job_info                   true
flush_submit_sec                  0
flush_finish_sec                  0
params                            none
reprioritize_interval             0:0:0
halftime                          168
usage_weight_list                 cpu=1.000000,mem=0.000000,io=0.000000
compensation_factor               5.000000
weight_user                       0.250000
weight_project                    0.250000
weight_department                 0.250000
weight_job                        0.250000
weight_tickets_functional         0
weight_tickets_share              0
share_override_tickets           TRUE
share_functional_shares          TRUE
max_functional_jobs_to_schedule  200
report_pjob_tickets             TRUE
max_pending_tasks_per_job       50
halflife_decay_list             none
policy_hierarchy                OFS
weight_ticket                   0
weight_waiting_time             0
weight_deadline                 3600000
weight_urgency                  0
weight_priority                 1" > scheduler.conf

# Apply the configuration
qconf -Msconf scheduler.conf

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
min_cpu_interval      00:00:01
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
notify                00:00:01
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

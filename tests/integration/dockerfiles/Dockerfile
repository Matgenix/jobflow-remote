# syntax=docker/dockerfile:experimental
ARG QUEUE_SYSTEM=slurm

# 1) Start from a Ubuntu server
FROM ubuntu:22.04 as base

# 3) Add desired queue system as a build stage:
#
# Each different queue system must provide an startup script, following the example
# of the prebuilt SLURM image, that needs to be run by a user with sudo priveleges.
# This script should also start the SSH server via `service ssh start`.
# This script is then edited at the final stage to include starting the SSH server and updating
# the jobflow user's permissions.
FROM nathanhess/slurm:full AS slurm
COPY ./tests/integration/dockerfiles/slurm_startup.sh /etc/startup.sh
CMD ["/bin/sh", "-c", "sudo ./etc/startup.sh; /bin/bash -l"]

FROM ubuntu:22.04 as sge
# Following instructions at https://peteris.rocks/blog/sun-grid-engine-installation-on-ubuntu-server/

COPY ./tests/integration/dockerfiles/sge_startup.sh /etc/startup.sh

RUN echo "gridengine-master       shared/gridenginemaster string  localhost" | debconf-set-selections
RUN echo "gridengine-master       shared/gridenginecell   string  default" | debconf-set-selections
RUN echo "gridengine-master       shared/gridengineconfig boolean false" | debconf-set-selections
RUN echo "gridengine-common       shared/gridenginemaster string  localhost" | debconf-set-selections
RUN echo "gridengine-common       shared/gridenginecell   string  default" | debconf-set-selections
RUN echo "gridengine-common       shared/gridengineconfig boolean false" | debconf-set-selections
RUN echo "gridengine-client       shared/gridenginemaster string  localhost" | debconf-set-selections
RUN echo "gridengine-client       shared/gridenginecell   string  default" | debconf-set-selections
RUN echo "gridengine-client       shared/gridengineconfig boolean false" | debconf-set-selections
RUN echo "postfix postfix/main_mailer_type        select  No configuration" | debconf-set-selections
RUN apt update && apt install -y gridengine-master gridengine-client gridengine-exec && apt clean && rm -rf /var/lib/apt/lists/*
USER sgeadmin
RUN /usr/share/gridengine/scripts/init_cluster /var/lib/gridengine default /var/spool/gridengine/spooldb sgeadmin
RUN service postfix disable
RUN update-rc.d postfix disable
RUN echo -e "group_name @allhosts\nhostlist NONE" > ./grid && qconf -Ahgrp ./grid && rm ./grid
RUN cat > ./grid $"qname jobflow.q\nhostlist @allhosts\nload_thresholds NONE\n" && qconf -Aq ./grid && rm ./grid

RUN qconf -as localhost
RUN qconf -aattr hostgrop hostlist localhost @allhosts
RUN qmod -e jobflow.q@localhost

FROM ${QUEUE_SYSTEM} as final

ARG USERNAME=jobflow
ARG PASSWORD=jobflow
WORKDIR /opt
USER root

# 3) Run an SSH server and set up Python environment and user for jobflow
# Install OpenSSH server and set it to run on startup
RUN apt update && apt install -y openssh-server sudo python3.10-venv && apt clean && rm -rf /var/lib/apt/lists/*
RUN sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config

# Create desired user with blank password then give user access to startup script as sudo without password
# See https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/Dockerfile#L62C1-L65C1
RUN useradd -rm -d /home/${USERNAME} -s /bin/bash ${USERNAME} && usermod -a -G sudo ${USERNAME}
RUN echo ${USERNAME}:${PASSWORD} | chpasswd
RUN printf "${USERNAME} ALL=(root:root) NOPASSWD: /etc/startup.sh\n" >> /etc/sudoers.d/startup \
    && chmod 0440 /etc/sudoers.d/startup \
    && visudo -c

# Reset workdir and make jobflow data directory
WORKDIR /home/${USERNAME}
USER ${USERNAME}
SHELL ["/bin/bash", "--login", "-c"]

# Install jobflow from directory, assuming container
# is built at the root of the jobflow repo
RUN mkdir jobflow-remote
COPY src/jobflow_remote jobflow-remote/src/jobflow_remote
COPY pyproject.toml jobflow-remote/

# versioningit refuses to install a package without its full git history
# so here we remove versioningit config from pyproject.toml as we don't need
# the full version number (which allows us to cache many more layers)
RUN sed -i '/\[tool.versioningit.vcs\]/,+3d' jobflow-remote/pyproject.toml

# Annoyingly we want to use this with the Python SDK
# which does not support buildkit yet
# so cannot use --chmod in the copy directly and
# we have to become root for this step
USER root
RUN sudo chmod -R 0777 jobflow-remote
USER ${USERNAME}

# Install jobflow in a local native virtualenv
WORKDIR /home/${USERNAME}/jobflow-remote
RUN python3 -m venv /home/${USERNAME}/.venv
RUN /home/${USERNAME}/.venv/bin/pip install -U pip
RUN /home/${USERNAME}/.venv/bin/pip install --verbose -e .

# Make a job directory for jobflow
WORKDIR /home/${USERNAME}
RUN mkdir jfr

# syntax=docker/dockerfile:experimental

# Using the slurm base image, run an ssh server and install jobflow
FROM nathanhess/slurm:full AS base
ARG USERNAME=jobflow
ARG PASSWORD=jobflow
WORKDIR /opt
USER root

# Install OpenSSH server and set it to run on startup
RUN apt update && apt install -y openssh-server && apt clean && rm -rf /var/lib/apt/lists/*
RUN sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
RUN sed -ie 's/^SCRIPT/service ssh start\nSCRIPT/g' /etc/startup.sh

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
# RUN git config --global --add safe.directory /home/${USERNAME}/jobflow-remote
RUN python3 -m venv /home/${USERNAME}/.venv
RUN /home/${USERNAME}/.venv/bin/pip install -U pip
RUN /home/${USERNAME}/.venv/bin/pip install --verbose -e .

# Make a job directory for jobflow
WORKDIR /home/${USERNAME}
RUN mkdir jfr

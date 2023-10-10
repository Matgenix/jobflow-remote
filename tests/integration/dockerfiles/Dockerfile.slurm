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
RUN sed -i 's/#PermitEmptyPasswords no/PermitEmptyPasswords yes/g' /etc/ssh/sshd_config
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

# Install micromamba to manage Python environments
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba
RUN micromamba shell init --shell bash --root-prefix ~/micromamba
RUN micromamba create --yes -n jobflow python=3.10 -c conda-forge

# Install jobflow from directory, assuming container 
# is built at the root of the jobflow repo
COPY . jobflow-remote

# Annoyingly we want to use this with the Python SDK
# which does not support buildkit yet
# so cannot use --chmod in the copy directly and 
# we have to become root for this step
USER root
RUN sudo chmod -R 0777 jobflow-remote
USER ${USERNAME}

WORKDIR /home/${USERNAME}/jobflow-remote
RUN git config --global --add safe.directory /home/${USERNAME}/jobflow-remote
RUN micromamba run -n jobflow pip install -e .

# Make a job directory for jobflow
WORKDIR /home/${USERNAME}
RUN mkdir jfr

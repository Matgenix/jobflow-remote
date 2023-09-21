# Using the slurm base image, run an ssh server
FROM nathanhess/slurm:full as base
ARG USERNAME=jobflow
ARG PASSWORD=jobflow

WORKDIR /opt
USER root

# Install OpenSSH server and set it to run on startup
RUN apt update && apt install -y openssh-server && apt clean && rm -rf /var/lib/apt/lists/*
RUN sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
RUN sed -i 's/#PermitEmptyPasswords no/PermitEmptyPasswords yes/g' /etc/ssh/sshd_config
RUN sed -ie 's/^SCRIPT/service ssh start\nSCRIPT/g' /etc/startup.sh

# Install MongoDB and set it to run on startup
RUN curl -fsSL https://pgp.mongodb.com/server-7.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
RUN echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
RUN apt update && apt install -y mongodb-org && apt clean && rm -rf /var/lib/apt/lists/*
RUN sed -ie 's/^SCRIPT/mongod \&\nSCRIPT/g' /etc/startup.sh
RUN mkdir -p /data/db

# Create desired user with blank password then give user access to startup script as sudo without password
# See https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/Dockerfile#L62C1-L65C1
RUN useradd -rm -d /home/${USERNAME} -s /bin/bash ${USERNAME} && usermod -a -G sudo ${USERNAME}
RUN echo ${USERNAME}:${PASSWORD} | chpasswd
RUN printf "${USERNAME} ALL=(root:root) NOPASSWD: /etc/startup.sh\n" >> /etc/sudoers.d/startup \
    && chmod 0440 /etc/sudoers.d/startup \
    && visudo -c


WORKDIR /home/${USERNAME}
USER ${USERNAME}
# Make a job directory for jobflow
RUN mkdir jfr


#from python:3.10 as base

#from base as nightly
# "nightly" image uses latest jobflow-remote on both runner and worker
#workdir /opt

# Mount current state of jobflow-remote dir and do an editable pip install
# (same dir must then be mounted on container launch)
#run --mount=target=/opt/app,type=bind,source=../../ pip install -e .

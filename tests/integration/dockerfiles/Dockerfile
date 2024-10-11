# syntax=docker/dockerfile:experimental
ARG QUEUE_SYSTEM=slurm
ARG USERNAME=jobflow

# Install jobflow-remote in its own layer with uv
FROM ubuntu:22.04 as jobflow-remote-install

ARG USERNAME=jobflow
ENV USERNAME=${USERNAME}

RUN apt update && \
    apt install -y python3.10-venv && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.4 /uv /usr/local/bin/uv

# As a bit of a hack, we make the path for this installation the same
# as the eventual .venv export, to avoid needing to patch the activation
# scripts with the correct path
WORKDIR /home/${USERNAME}/jobflow-remote
COPY src/jobflow_remote /home/${USERNAME}/jobflow-remote/src/jobflow_remote
COPY pyproject.toml /home/${USERNAME}/jobflow-remote/
COPY requirements/requirements.txt /home/${USERNAME}/jobflow-remote/requirements/

# versioningit refuses to install a package without its full git history
# so here we remove versioningit config from pyproject.toml as we don't need
# the full version number (which allows us to cache many more layers)
RUN sed -i '/\[tool.versioningit.vcs\]/,+3d' /home/${USERNAME}/jobflow-remote/pyproject.toml

WORKDIR /home/${USERNAME}/jobflow-remote
RUN uv venv /home/${USERNAME}/.venv && \
    uv pip install --python /home/${USERNAME}/.venv/bin/python -r /home/${USERNAME}/jobflow-remote/requirements/requirements.txt && \
    uv pip install --python /home/${USERNAME}/.venv/bin/python --no-deps .

# Add desired queue system as a build stage:
# Each different queue system must provide an startup script, following the example
# of the prebuilt SLURM image, that needs to be run by a user with sudo priveleges.
# This script should also start the SSH server via `service ssh start`.
# This script is then edited at the final stage to include starting the SSH server and updating
# the jobflow user's permissions.

# -== SLURM ==-
FROM nathanhess/slurm:full AS slurm
USER root
COPY ./tests/integration/dockerfiles/slurm_startup.sh /etc/startup.sh
HEALTHCHECK CMD sinfo || exit 1

# -== SGE ==-
FROM ubuntu:22.04 as sge

ARG USERNAME=jobflow
ENV USERNAME=${USERNAME}
USER root

## Following instructions from https://github.com/daimh/sge/blob/817a7fa019f0ec47425f43e843a1f58865614313/README.md
RUN apt update && \
    apt install -y \
        sudo \
        build-essential \
        curl \
        cmake \
        git \
        libdb5.3-dev \
        libhwloc-dev \
        libmotif-dev \
        libncurses-dev \
        libpam0g-dev \
        libssl-dev \
        libsystemd-dev \
        libtirpc-dev \
        libxext-dev \
        pkgconf && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN git clone https://github.com/daimh/sge --depth 1 --branch master sge
WORKDIR /tmp/sge
RUN cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/opt/sge && \
    cmake --build build -j && \
    cmake --install build

# Create the user and add them to the `sge` group
RUN groupadd sge && \
    useradd -rm -d /home/${USERNAME} -s /bin/bash ${USERNAME} && \
    usermod -a -G sudo,sge ${USERNAME} && \
    chown -R ${USERNAME} /opt/sge

WORKDIR /opt/sge

# Give the user passwordless sudo access to the SGE installation scripts
RUN printf "${USERNAME} ALL=(root:root) NOPASSWD: /opt/sge/install_qmaster\n" >> /etc/sudoers.d/startup \
    && chmod 0440 /etc/sudoers.d/startup \
    && visudo -c
RUN printf "${USERNAME} ALL=(root:root) NOPASSWD: /opt/sge/install_execd\n" >> /etc/sudoers.d/startup \
    && chmod 0440 /etc/sudoers.d/startup \
    && visudo -c

## The SGE startup script includes the configuration of SGE, as the docker network is not ready during build
COPY ./tests/integration/dockerfiles/sge_startup.sh /etc/startup.sh

HEALTHCHECK CMD qstat || exit 1

# 2) Add common options from ubuntu server
FROM ${QUEUE_SYSTEM} as jobflow

ARG USERNAME=jobflow
ENV USERNAME=${USERNAME}

WORKDIR /opt
USER root

# 3) Run an SSH server and set up Python environment and user for jobflow
# Install OpenSSH server and set it to run on startup
RUN apt update && \
    apt install -y \
        dos2unix \
        openssh-server \
        sudo \
        python3.10-venv && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

RUN sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config

# If they do not exist already, create the desired user with blank password then give user access to startup script as sudo without password
# See https://github.com/nathan-hess/docker-slurm/blob/a62133d66d624d9ff0ccefbd41a0b1b2abcb9925/dockerfile_base/Dockerfile#L62C1-L65C1
RUN useradd -rm -d /home/${USERNAME} -s /bin/bash ${USERNAME} || exit 0 && \
    usermod -a -G sudo ${USERNAME}
RUN echo ${USERNAME}:${USERNAME} | chpasswd
RUN printf "${USERNAME} ALL=(root:root) NOPASSWD: /etc/startup.sh\n" >> /etc/sudoers.d/startup \
    && chmod 0440 /etc/sudoers.d/startup \
    && visudo -c

# Make a job directory for jobflow
WORKDIR /home/${USERNAME}
RUN mkdir jfr

# Copy the installed jobflow-remote virtualenv
COPY --from=jobflow-remote-install /home/${USERNAME}/.venv /home/${USERNAME}/.venv

# Finalize permissions on startup script and jobflow installation
USER root
RUN chmod +x /etc/startup.sh && \
    dos2unix /etc/startup.sh
RUN chown -R ${USERNAME} /home/${USERNAME} && \
    chmod -R 755 /home/${USERNAME}
USER ${USERNAME}

CMD sudo /etc/startup.sh ; /bin/bash -l

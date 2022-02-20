#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
IMAGE_NAME="roscap-${USER}"
CONTAINER_NAME="roscap-${USER}"

echo "Running docker build ${IMAGE_NAME}"

USERID=$(id -u)
GROUPID=$(id -g)

docker build -t ${IMAGE_NAME} - << EOF
FROM ros:noetic-ros-base

ARG DEBIAN_FRONTEND=noninteractive

# Update the OS and add dependencies plus some useful packages
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      libcrypto++-dev libfmt-dev liblz4-dev libzstd-dev \
      lldb git nano vim

# Create the same user in Docker as the host user
RUN useradd -ms /bin/bash ${USER} -u ${USERID}
RUN mkdir -p ${HOME} && \
    mkdir -p /etc/sudoers.d && \
    echo "${USER}:x:${USERID}:${GROUPID}:${USER},,,:${HOME}:/bin/bash" >> /etc/passwd && \
    echo "${USER}:x:${GROUPID}:" >> /etc/group && \
    echo "${USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/${USER} && \
    chmod 0440 /etc/sudoers.d/${USER} && \
    chown -R ${USER}:${USER} ${HOME}

USER ${USER}
ENV HOME ${HOME}
WORKDIR ${SCRIPT_DIR}

EOF

echo "Stopping any running containers"

docker stop ${CONTAINER_NAME}
docker rm ${CONTAINER_NAME}

echo "Running docker create ${IMAGE_NAME}"

docker create -it \
  --name ${CONTAINER_NAME} \
  --user ${USER} \
  --privileged \
  --cap-add=SYS_PTRACE \
  --security-opt seccomp=unconfined \
  -v ${HOME}:${HOME} \
  -v /dev:/dev \
  -v /etc/localtime:/etc/localtime:ro \
  -v /etc/timezone:/etc/timezone:ro \
  -e DISPLAY=${DISPLAY} \
  -e CONTAINER_NAME=${CONTAINER_NAME} \
  --hostname ${CONTAINER_NAME} \
  ${IMAGE_NAME}

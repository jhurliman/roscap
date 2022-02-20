#!/usr/bin/env bash

CONTAINER_NAME="roscap-${USER}"

echo "Running docker start ${CONTAINER_NAME}"

docker start ${CONTAINER_NAME}
docker exec -it -e COLUMNS=$(tput cols) -e LINES=$(tput lines) ${CONTAINER_NAME} bash

#!/bin/bash

# https://gist.github.com/GhazanfarMir/da901b7fcdec40f07a5ba221372862eb

echo 'Stopping running containers (if any)...'
docker stop $(docker ps -aq)

echo 'Clean mountpoint folders'
rm -r mountpoint/dags/ mountpoint/logs/ mountpoint/plugins

echo 'Update mountpoint folders'
/bin/bash copy_dags.sh

echo 'Removing containers ..'
docker rm $(docker ps -aq)

echo 'Removing images ...'
docker rmi $(docker images -q)

echo 'Removing docker container volumes (if any)'
docker volume rm $(docker volume ls -q)

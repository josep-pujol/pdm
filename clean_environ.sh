#!/bin/bash

# https://gist.github.com/GhazanfarMir/da901b7fcdec40f07a5ba221372862eb

echo ''
echo 'Starting to clean the environment'
echo '---------------------------------'

echo ''
echo '- Stop running containers (if any)'
docker stop $(docker ps -aq)

echo ''
echo '- Clean mountpoint folders'
rm -r mountpoint/dags/
rm -r mountpoint/logs/
rm -r mountpoint/plugins/

echo ''
echo '- Removing containers'
docker rm $(docker ps -aq)

echo ''
echo '- Removing images'
docker rmi $(docker images -q)

echo ''
echo '- Removing docker container volumes (if any)'
docker volume rm $(docker volume ls -q)

echo ''
echo '- Update mountpoint folders'
/bin/bash copy_dags.sh

echo ''
echo 'Cleaning environment completed.'

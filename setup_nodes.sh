#!/bin/bash

# build the image with MongoDB, Flask, Python
docker build -t node ./docker

# start containers
docker run -dit --name taskpool -h taskpool \
	-v /YOUR_LOCAL_PATH/task-scheduler:/root/task-scheduler node
docker run -dit --name master -h master \
	-v /YOUR_LOCAL_PATH/task-scheduler:/root/task-scheduler node
docker run -dit --name worker1 -h worker1 \
	-v /YOUR_LOCAL_PATH/task-scheduler:/root/task-scheduler node
docker run -dit --name worker2 -h worker2 \
	-v /YOUR_LOCAL_PATH/task-scheduler:/root/task-scheduler node
docker run -dit --name worker3 -h worker3 \
	-v /YOUR_LOCAL_PATH/task-scheduler:/root/task-scheduler node

# In each container, run: service mongodb start to start a mongodb service 
# on local host. might need to manullly install MongoDB if this doesn`t work

# On docker host machine, add the hostname of each container to /etc/hosts to 
# let them find each other. For example: echo 172.17.0.2 taskpool>> /etc/hosts
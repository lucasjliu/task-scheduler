#
# MongoDB, Flask, Python3
#
FROM ubuntu:16.04

MAINTAINER Jiahuan Liu "jiahuan.liu@outlook.com"

#RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6 \
#  && echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 Release" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list
# Some build issues happened using latest MongoDB above,
# so instead used an older but stable one

RUN mkdir /data
RUN mkdir /data/db
RUN apt-get update
RUN apt-get install -y mongodb
RUN apt-get install -y python3 python3-pip vim
RUN apt-get clean
RUN pip3 install pymongo flask

WORKDIR /root/task-scheduler
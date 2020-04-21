FROM ubuntu:latest

WORKDIR /usr/local/data

RUN apt update && apt install -y python3-pip python3.6-dev

RUN pip3 install future oedtools

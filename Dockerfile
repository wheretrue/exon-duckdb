FROM ubuntu:20.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install python3 python3-pip build-essential python-dev unzip

RUN pip3 install duckdb==0.7.0

ADD ./wtt01.duckdb_extension /tmp/wtt01.duckdb_extension

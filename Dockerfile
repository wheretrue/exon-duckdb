FROM ubuntu:20.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install python3 python3-pip build-essential python-dev unzip wget

RUN wget -O /tmp/duckdb.zip https://github.com/duckdb/duckdb/releases/download/v0.7.0/duckdb_cli-linux-amd64.zip
RUN unzip /tmp/duckdb.zip -d /usr/local/bin

RUN pip3 install duckdb==0.7.0 ipython pandas

ADD ./wtt01.duckdb_extension /tmp/wtt01.duckdb_extension

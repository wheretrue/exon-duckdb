FROM ubuntu:20.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive

ARG CHECK_LICENSE
ENV CHECK_LICENSE=${CHECK_LICENSE}

ARG WTT_01_LICENSE_SERVER_URL
ENV WTT_01_LICENSE_SERVER_URL=${WTT_01_LICENSE_SERVER_URL}

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install python3 python3-pip build-essential python-dev

ADD ./wtt01.duckdb_extension /tmp/wtt01.duckdb_extension

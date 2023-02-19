FROM ubuntu:latest AS builder

ARG DEBIAN_FRONTEND=noninteractive

ARG CHECK_LICENSE
ENV CHECK_LICENSE=${CHECK_LICENSE}

ARG WTT_01_LICENSE_SERVER_URL
ENV WTT_01_LICENSE_SERVER_URL=${WTT_01_LICENSE_SERVER_URL}

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install python3 python3-pip build-essential

RUN apt-get install -y -qq software-properties-common && \
        add-apt-repository ppa:git-core/ppa && \
        apt-get update -y -qq && \
        apt-get install -y -qq ninja-build make gcc-multilib g++-multilib libssl-dev wget openjdk-8-jdk zip maven unixodbc-dev libc6-dev-i386 lib32readline6-dev libssl-dev libcurl4-gnutls-dev libexpat1-dev gettext unzip build-essential checkinstall libffi-dev curl libz-dev openssh-client ccache git

RUN wget https://github.com/Kitware/CMake/releases/download/v3.25.2/cmake-3.25.2-linux-x86_64.sh && \
        chmod +x cmake-3.25.2-linux-x86_64.sh && \ 
        ./cmake-3.25.2-linux-x86_64.sh --skip-license --prefix=/usr/local && \
        cmake --version

RUN curl https://sh.rustup.rs -sSf > rustup.sh && \
        sh rustup.sh -y

ENV PATH="/root/.cargo/bin:${PATH}"

COPY ./ /app
WORKDIR /app

RUN make release

WORKDIR /tmp
ENTRYPOINT ["/app/build/release/duckdb"]

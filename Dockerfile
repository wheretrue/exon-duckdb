FROM ubuntu:latest AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install python3 python3-pip build-essential

RUN apt-get install -y -qq software-properties-common && \
        add-apt-repository ppa:git-core/ppa && \
        apt-get update -y -qq && \
        apt-get install -y -qq ninja-build make libssl-dev zip unzip checkinstall libffi-dev curl libz-dev ccache git wget

ARG PLATFORM
ENV PLATFORM=${PLATFORM}
RUN wget -O cmake.sh https://github.com/Kitware/CMake/releases/download/v3.25.2/cmake-3.25.2-linux-${PLATFORM}.sh && \
        chmod +x cmake.sh && \
        ./cmake.sh --skip-license --prefix=/usr/local && \
        cmake --version

RUN curl https://sh.rustup.rs -sSf > rustup.sh && \
        sh rustup.sh -y

ENV PATH="/root/.cargo/bin:${PATH}"

# install micromamba
RUN wget micro.mamba.pm/install.sh
RUN bash install.sh
ENV PATH /root/.local/bin/:$PATH

COPY ./environment.yml /tmp/environment.yml
# RUN micromamba create -n wtt01 -f /tmp/environment.yml

# RUN apt-get install -y -qq pkg-config

# FROM builder AS extension_builder

# COPY ./ /app
# WORKDIR /app

# ARG CHECK_LICENSE
# ENV CHECK_LICENSE=${CHECK_LICENSE}

# ARG WTT_01_LICENSE_SERVER_URL
# ENV WTT_01_LICENSE_SERVER_URL=${WTT_01_LICENSE_SERVER_URL}

# RUN make release

# # FROM builder AS duckdb

# # COPY --from=extension_builder /app/build/release/duckdb /usr/local/bin/duckdb
# # COPY --from=extension_builder /app/build/release/extension/wtt01/wtt01.duckdb_extension /wtt01.duckdb_extension

# # WORKDIR /tmp
# # ENTRYPOINT ["/usr/local/bin/duckdb", "-unsigned"]

.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_PARQUET_EXTENSION=1 ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=-DDUCKDB_OOT_EXTENSION_NAMES="exondb" -DDUCKDB_OOT_EXTENSION_EXONDB_PATH="$(PROJ_DIR)" -DDUCKDB_OOT_EXTENSION_EXONDB_SHOULD_LINK="TRUE" -DDUCKDB_OOT_EXTENSION_EXONDB_INCLUDE_PATH="$(PROJ_DIR)src/include"

pull:
	git submodule init
	git submodule update --recursive --remote

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release -j 8 --target 'cargo-build_wtt01_rust' && \
	cmake --build build/release --config Release -j 8

release-windows:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release -j 8 --target 'cargo-build_wtt01_rust' && \
	cmake --build build/release --config Release -j 8

debug:
	mkdir -p build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Release --target 'cargo-build_wtt01_rust' && \
	cmake --build build/debug --config Release -j 8 --target 'htslib' && \
	cmake --build build/debug --config Debug -j 8 --target all

test_debug:
	mkdir -p ./test/sql/tmp/
	rm -rf ./test/sql/tmp/*
	./build/debug/test/unittest --test-dir . "[wtt-01-release-with-deb-info]"
	rm -rf ./test/sql/tmp

release_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
release_python: release

# Main tests
test: test_release

test_release: release
	mkdir -p ./test/sql/tmp/
	rm -rf ./test/sql/tmp/*
	./build/release/test/unittest --test-dir . "[wtt-01-release-with-deb-info]"
	rm -rf ./test/sql/tmp

test_align:
	./build/release/test/unittest --test-dir . "[wtt-01-align]"

update:
	git submodule update --remote --merge

r:
	mkdir -p r-dist
	R CMD build wtt01r
	mv wtt01r*tar.gz r-dist/
	aws s3 cp --recursive r-dist/ s3://wtt-01-dist-$(ENVIRONMENT)/R/
	rm -rf r-dist


AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text)

docker-release:
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
	DOCKER_IMAGE=public.ecr.aws/p3a4z1t3/wtt01:latest PLATFORM=${PLATFORM} docker compose build wtt01
	docker tag public.ecr.aws/p3a4z1t3/wtt01:latest public.ecr.aws/p3a4z1t3/wtt01:v0.3.9
	docker push public.ecr.aws/p3a4z1t3/wtt01:latest
	docker push public.ecr.aws/p3a4z1t3/wtt01:v0.3.9

extension-release:
	ENVIRONMENT=$(ENVIRONMENT) python bin/upload-artifacts.py

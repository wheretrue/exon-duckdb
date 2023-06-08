.PHONY: all clean release duckdb_release pull update

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
	cmake --build build/release --config Release -j 8 --target 'cargo-build_rust' && \
	cmake --build build/release --config Release -j 8

release-windows:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release -j 8 --target 'cargo-build_rust' && \
	cmake --build build/release --config Release -j 8

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

extension-release:
	ENVIRONMENT=$(ENVIRONMENT) python bin/upload-artifacts.py

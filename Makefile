pull:
	git submodule init
	git submodule update --recursive --remote


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

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP}
ifeq (${BUILD_SHELL}, 0)
	BUILD_FLAGS += -DBUILD_SHELL=0
endif

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=-DENABLE_SANITIZER=OFF -DDUCKDB_OOT_EXTENSION_NAMES="exon" -DDUCKDB_OOT_EXTENSION_EXON_PATH="$(PROJ_DIR)" -DDUCKDB_OOT_EXTENSION_EXON_SHOULD_LINK="TRUE" -DDUCKDB_OOT_EXTENSION_EXON_INCLUDE_PATH="$(PROJ_DIR)exon/include"

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release -j 8 --target 'cargo-build_rust' && \
	cmake --build build/release --config Release

test: release
	mkdir -p ./test/sql/tmp/
	rm -rf ./test/sql/tmp/*
	./build/release/test/unittest --test-dir . "[exondb-release-with-deb-info]"
	rm -rf ./test/sql/tmp

test_windows: release
	mkdir -p ./test/sql/tmp/
	rm -rf ./test/sql/tmp/*
	./build/release/test/Release/unittest.exe --test-dir . "[exondb-release-with-deb-info]"
	rm -rf ./test/sql/tmp

extension-release:
    ENVIRONMENT=$(ENVIRONMENT) python bin/upload-artifacts.py

cmake_minimum_required(VERSION 3.21)

# Set extension name here
set(TARGET_NAME wtt01)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})

# If the env var WTT_SPDLOG_ACTIVE_LEVEL is not set, set it to warning, otherwise use the value of the env var.
set(WTT_SPDLOG_ACTIVE_LEVEL $ENV{WTT_SPDLOG_ACTIVE_LEVEL})
if (WTT_SPDLOG_ACTIVE_LEVEL)
  add_definitions(-DSPDLOG_ACTIVE_LEVEL=1)
  message("-- wtt: WTT_SPDLOG_ACTIVE_LEVEL is ${WTT_SPDLOG_ACTIVE_LEVEL}")
else()
  add_definitions(-DSPDLOG_ACTIVE_LEVEL=3)
  message("-- wtt: WTT_SPDLOG_ACTIVE_LEVEL is not set")
endif()

if (APPLE)
  execute_process(COMMAND brew --prefix openssl@3 OUTPUT_VARIABLE OPENSSL_PREFIX)
  string(STRIP ${OPENSSL_PREFIX} OPENSSL_ROOT_DIR)

  include_directories(${OPENSSL_ROOT_DIR}/include)
  link_directories(${OPENSSL_ROOT_DIR}/lib)

  set(WFA2_ENABLED ON)
  set(HTSLIB_ENABLED ON)

  add_compile_definitions(WFA2_ENABLED)
  add_compile_definitions(HTSLIB_ENABLED)

elseif(UNIX)
  set(OPENSSL_ROOT_DIR /usr/local/ssl)

  set(WFA2_ENABLED ON)
  set(HTSLIB_ENABLED ON)

  add_compile_definitions(WFA2_ENABLED)
  add_compile_definitions(HTSLIB_ENABLED)

  include_directories(${OPENSSL_ROOT_DIR}/include)
  link_directories(${OPENSSL_ROOT_DIR}/lib)

elseif(WIN32)
  set(WFA2_ENABLED OFF)
endif()

find_package(OpenSSL REQUIRED)
message("-- wtt: Found openssl ${OPENSSL_VERSION}")

set(CHECK_LICENSE $ENV{CHECK_LICENSE})
if (CHECK_LICENSE)
  add_compile_definitions(CHECK_LICENSE)
  message("-- wtt: CHECK_LICENSE is ${CHECK_LICENSE}")
else()
  message("-- wtt: CHECK_LICENSE is not set")
endif()

set(WTT_01_LICENSE_SERVER_URL $ENV{WTT_01_LICENSE_SERVER_URL})
if (WTT_01_LICENSE_SERVER_URL)
  add_definitions(-DWTT_01_LICENSE_SERVER_URL="${WTT_01_LICENSE_SERVER_URL}")
  message("-- wtt: WTT_01_LICENSE_SERVER_URL is ${WTT_01_LICENSE_SERVER_URL}")
else()
  message("-- wtt: WTT_01_LICENSE_SERVER_URL is not set")
endif()

Include(FetchContent)

FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.12.0
)

FetchContent_Declare(
  json
  URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz
)

FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.3.2
)

FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.11.0
)

list(APPEND available_contents httplib json Corrosion spdlog)

FetchContent_MakeAvailable(${available_contents})

corrosion_import_crate(MANIFEST_PATH wtt01_rust/Cargo.toml
  PROFILE release
)

message("-- wtt source: ${CMAKE_CURRENT_SOURCE_DIR}")
message("-- wtt include: ${CMAKE_INSTALL_INCLUDEDIR}")
message("-- wtt libdir: ${CMAKE_INSTALL_LIBDIR}")
message("-- wtt runtime: ${CMAKE_INSTALL_BINDIR}")
message("-- wtt: CMAKE_CURRENT_SOURCE_DIR is ${CMAKE_CURRENT_SOURCE_DIR}")
message("-- wtt: CMAKE_CURRENT_BINARY_DIR is ${CMAKE_CURRENT_BINARY_DIR}")
message("-- wtt: PROJECT_BINARY_DIR is ${PROJECT_BINARY_DIR}")

include_directories(${json_SOURCE_DIR}/include)
include_directories(${httplib_SOURCE_DIR})
include_directories(${arrow_SOURCE_DIR}/cpp/src)
include_directories(${spdlog_SOURCE_DIR}/include)

# add CONDA_PREFIX to cmake search path
list(APPEND CMAKE_PREFIX_PATH $ENV{CONDA_PREFIX})

message("-- wtt: WFA2_ENABLED is ${WFA2_ENABLED}")
if(WFA2_ENABLED)
  FetchContent_Declare(wfa2
    GIT_REPOSITORY https://github.com/tshauck/WFA2-lib.git
    GIT_TAG 4784d5892c25f1967174b4deb7a3a7f1f34a9cb8
  )

  FetchContent_MakeAvailable(wfa2)
  include_directories(${wfa2_SOURCE_DIR})
endif()

message("-- wtt: HTSLIB_ENABLED is ${HTSLIB_ENABLED}")
if(HTSLIB_ENABLED)
  include(ExternalProject)
  set(HTSLIB_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/htslib)

  set(flags "-O2 -g -fPIC")
  ExternalProject_Add(htslib
    PREFIX ${HTSLIB_BUILD_DIR}
    GIT_REPOSITORY https://github.com/samtools/htslib.git
    GIT_TAG 1.17
    BUILD_IN_SOURCE ON
    UPDATE_COMMAND autoreconf -i
    CONFIGURE_COMMAND ./configure --prefix=${HTSLIB_BUILD_DIR}
    BUILD_COMMAND make CFLAGS=${flags} lib-static
    INSTALL_COMMAND cd ${HTSLIB_BUILD_DIR}/src/htslib && make install
  )
  # add_library(HTSLIB_LIBRARIES STATIC IMPORTED)
  # set_target_properties(HTSLIB_LIBRARIES PROPERTIES IMPORTED_LOCATION ${htslib_lib})
  # add_dependencies(HTSLIB_LIBRARIES htslib-EXT)

  include_directories(${HTSLIB_BUILD_DIR}/include)

endif()

include_directories($ENV{CONDA_PREFIX}/include)
include_directories(src/include)

set(EXTENSION_SOURCES
  src/wtt01_extension.cpp
  src/check_license.cpp

  src/bam_io.cpp
  src/bed_io.cpp
  src/fasta_io.cpp
  src/fastq_io.cpp
  src/genbank_io.cpp
  src/gff_io.cpp
  src/hmm_io.cpp
  src/sam_cigar_parse.cpp
  src/sam_io.cpp
  src/sequence_functions.cpp
  src/wtt01_acknowledgement_table.cpp
  src/wtt01_functions.cpp
)

if(WFA2_ENABLED)
  set(EXTENSION_SOURCES ${EXTENSION_SOURCES}
    src/alignment_functions.cpp
  )
endif()

if(HTSLIB_ENABLED)
  set(EXTENSION_SOURCES ${EXTENSION_SOURCES}
    src/vcf_io_types.cpp
  )
endif()

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")

build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

if (WIN32)
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/Release/wtt01_rust.lib"
    OpenSSL::SSL OpenSSL::Crypto bcrypt Userenv)

elseif(APPLE)

  find_package(LibLZMA REQUIRED)
  find_package(libdeflate REQUIRED)

  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/libwtt01_rust.a"

    "${wfa2_BINARY_DIR}/libwfa2cpp.a"
    "${wfa2_BINARY_DIR}/libwfa2.a"
    "-framework CoreFoundation"

    ${OPENSSL_LIBRARIES}

    $ENV{CONDA_PREFIX}/lib/libhts.a
    $ENV{CONDA_PREFIX}/lib/libcurl.a

    $ENV{CONDA_PREFIX}/lib/libz.a
    $ENV{CONDA_PREFIX}/lib/libbz2.a

    # link liblzma
    ${LIBLZMA_LIBRARIES}
    ${LIBDEFLATE_LIBRARIES}

  )

else()
  find_package(LibLZMA REQUIRED)
  find_package(libdeflate REQUIRED)

  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/libwtt01_rust.a"
    "${wfa2_BINARY_DIR}/libwfa2cpp.a"
    "${wfa2_BINARY_DIR}/libwfa2.a"
    ${OPENSSL_LIBRARIES}

    $ENV{CONDA_PREFIX}/lib/libhts.a
    $ENV{CONDA_PREFIX}/lib/libcurl.a

    $ENV{CONDA_PREFIX}/lib/libz.a
    $ENV{CONDA_PREFIX}/lib/libbz2.a

    # link liblzma
    ${LIBLZMA_LIBRARIES}

    ${LIBDEFLATE_LIBRARIES}
    -ldeflate
  )
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

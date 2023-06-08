cmake_minimum_required(VERSION 3.21)

# Set extension name here
set(TARGET_NAME exondb)

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
  add_compile_definitions(WFA2_ENABLED)

elseif(UNIX)
  set(OPENSSL_ROOT_DIR /usr/local/ssl)

  set(WFA2_ENABLED ON)
  add_compile_definitions(WFA2_ENABLED)

  include_directories(${OPENSSL_ROOT_DIR}/include)
  link_directories(${OPENSSL_ROOT_DIR}/lib)

elseif(WIN32)
  set(WFA2_ENABLED OFF)
endif()

find_package(OpenSSL REQUIRED)
message("-- wtt: Found openssl ${OPENSSL_VERSION}")

Include(FetchContent)

FetchContent_Declare(
  arrow
  GIT_REPOSITORY https://github.com/apache/arrow.git
  GIT_TAG apache-arrow-11.0.0
)

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
  GIT_TAG v0.3.5
)

FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.11.0
)

list(APPEND available_contents httplib json Corrosion spdlog arrow)

FetchContent_MakeAvailable(${available_contents})

corrosion_import_crate(MANIFEST_PATH rust/Cargo.toml
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

message("-- wtt: WFA2_ENABLED is ${WFA2_ENABLED}")
if(WFA2_ENABLED)
  FetchContent_Declare(wfa2
    GIT_REPOSITORY https://github.com/tshauck/WFA2-lib.git
    GIT_TAG 4784d5892c25f1967174b4deb7a3a7f1f34a9cb8
  )

  FetchContent_MakeAvailable(wfa2)
  include_directories(${wfa2_SOURCE_DIR})
endif()

include_directories(src/include)

set(EXTENSION_SOURCES
  src/exondb_extension.cpp

  src/gff_io.cpp
  src/sam_io.cpp
  src/fastq_io.cpp
  src/sequence_functions.cpp
  src/wtt01_acknowledgement_table.cpp
  src/wtt01_functions.cpp
  src/wt_arrow_table_function.cpp
)

if(WFA2_ENABLED)
  set(EXTENSION_SOURCES ${EXTENSION_SOURCES}
    src/alignment_functions.cpp
  )
endif()

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")

build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

if (WIN32)
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/Release/rust.lib"
    ntdll
    Secur32
    OpenSSL::SSL
    OpenSSL::Crypto
    bcrypt
    ncrypt
    Userenv)

elseif(APPLE)
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/librust.a"

    "${wfa2_BINARY_DIR}/libwfa2cpp.a"
    "${wfa2_BINARY_DIR}/libwfa2.a"

    -lz
    -lm
    -lbz2
    /opt/homebrew/Cellar/xz/5.4.3/lib/liblzma.a
    -lcurl
    ${OPENSSL_LIBRARIES}

    "-framework CoreFoundation"
    "-framework Security"
    "-framework Accelerate"
  )

else()
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/librust.a"
    "${wfa2_BINARY_DIR}/libwfa2cpp.a"
    "${wfa2_BINARY_DIR}/libwfa2.a"

    -lz -lm -lbz2 -llzma -lcurl
    ${OPENSSL_LIBRARIES}
  )
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

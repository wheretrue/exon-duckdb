# cmake_minimum_required(VERSION 2.8.12)
cmake_minimum_required(VERSION 3.24)

# Set extension name here
set(TARGET_NAME wtt01)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})

Include(FetchContent)

FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.11.2)

FetchContent_Declare(
  json
  URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz
)

list(APPEND available_contents httplib json)

FetchContent_MakeAvailable(${available_contents})
include_directories(${json_SOURCE_DIR}/include)
include_directories(${httplib_SOURCE_DIR})

include_directories(src/include)
set(EXTENSION_SOURCES
  src/wtt01_extension.cpp
  src/check_license.cpp
  src/sequence_functions.cpp
)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

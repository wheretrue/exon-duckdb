cmake_minimum_required(VERSION 3.21)

# Set extension name here
set(TARGET_NAME wtt01)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})

if (APPLE)
  execute_process(COMMAND brew --prefix openssl@3 OUTPUT_VARIABLE OPENSSL_PREFIX)
  string(STRIP ${OPENSSL_PREFIX} OPENSSL_ROOT_DIR)

  include_directories(${OPENSSL_ROOT_DIR}/include)
  link_directories(${OPENSSL_ROOT_DIR}/lib)
endif()

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

find_package(OpenSSL REQUIRED)

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
target_link_libraries(${EXTENSION_NAME} PRIVATE OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

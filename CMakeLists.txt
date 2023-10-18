cmake_minimum_required(VERSION 3.2)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set extension name here
set(TARGET_NAME exon)

if (APPLE)
  execute_process(COMMAND brew --prefix xz OUTPUT_VARIABLE XZ_PREFIX)
  string(STRIP ${XZ_PREFIX} XZ_ROOT_DIR)

  set(WFA2_ENABLED OFF)
  # add_compile_definitions(WFA2_ENABLED)

elseif(UNIX)
  set(WFA2_ENABLED ON)
  add_compile_definitions(WFA2_ENABLED)

elseif(WIN32)
  set(WFA2_ENABLED OFF)
endif()

Include(FetchContent)

FetchContent_Declare(
  arrow
  GIT_REPOSITORY https://github.com/apache/arrow.git
  GIT_TAG apache-arrow-11.0.0
)

FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.4.4
)

list(APPEND available_contents Corrosion arrow)

FetchContent_MakeAvailable(${available_contents})

if(WFA2_ENABLED)
  FetchContent_Declare(wfa2
    GIT_REPOSITORY https://github.com/tshauck/WFA2-lib.git
    GIT_TAG 4784d5892c25f1967174b4deb7a3a7f1f34a9cb8
  )

  FetchContent_MakeAvailable(wfa2)
  include_directories(${wfa2_SOURCE_DIR})
endif()

corrosion_import_crate(MANIFEST_PATH exon-duckdb/Cargo.toml
  PROFILE release
)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(exon/include)
add_subdirectory(exon/src)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# print the CMAKE_CURRENT_BINARY_DIR
message(STATUS "CMAKE_CURRENT_BINARY_DIR: ${CMAKE_CURRENT_BINARY_DIR}")

target_link_libraries(${EXTENSION_NAME} PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}/libexon_duckdb.a
  # ${WFA2_LIBRARIES}
  # "${wfa2_BINARY_DIR}/libwfa2cpp.a"
  # "${wfa2_BINARY_DIR}/libwfa2.a"
  -lz
  -lm
  -lbz2
  ${XZ_ROOT_DIR}/lib/liblzma.a
  -lcurl
  OpenSSL::SSL
  OpenSSL::Crypto
  "-framework CoreFoundation"
  "-framework Security"
  "-framework Accelerate"
  "-framework SystemConfiguration"
)

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${LOADABLE_EXTENSION_NAME}
  ${CMAKE_CURRENT_BINARY_DIR}/libexon_duckdb.a
  # ${WFA2_LIBRARIES}
  # "${wfa2_BINARY_DIR}/libwfa2cpp.a"
  # "${wfa2_BINARY_DIR}/libwfa2.a"
  -lz
  -lm
  -lbz2
  ${XZ_ROOT_DIR}/lib/liblzma.a
  -lcurl
  OpenSSL::SSL
  OpenSSL::Crypto
  "-framework CoreFoundation"
  "-framework Security"
  "-framework Accelerate"
  "-framework SystemConfiguration"
)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

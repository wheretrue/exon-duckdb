cmake_minimum_required(VERSION 3.2)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set extension name here
set(TARGET_NAME exon)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(CMAKE_CXX_STANDARD 11)

project(${TARGET_NAME})

if (APPLE)
  execute_process(COMMAND brew --prefix openssl@3 OUTPUT_VARIABLE OPENSSL_PREFIX)
  string(STRIP ${OPENSSL_PREFIX} OPENSSL_ROOT_DIR)

  include_directories(${OPENSSL_ROOT_DIR}/include)
  link_directories(${OPENSSL_ROOT_DIR}/lib)

  execute_process(COMMAND brew --prefix xz OUTPUT_VARIABLE XZ_PREFIX)
  string(STRIP ${XZ_PREFIX} XZ_ROOT_DIR)

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

if(WFA2_ENABLED)
  FetchContent_Declare(wfa2
    GIT_REPOSITORY https://github.com/tshauck/WFA2-lib.git
    GIT_TAG 4784d5892c25f1967174b4deb7a3a7f1f34a9cb8
  )

  FetchContent_MakeAvailable(wfa2)
  include_directories(${wfa2_SOURCE_DIR})
endif()

corrosion_import_crate(MANIFEST_PATH rust/Cargo.toml
  PROFILE release
)

include_directories(exon/include)
add_subdirectory(exon/src)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Build extensions
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

find_package(OpenSSL REQUIRED)
message("-- wtt: Found openssl ${OPENSSL_VERSION}")

if(WIN32)
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/Release/rust.lib"
    ntdll
    Secur32
    OpenSSL::SSL
    OpenSSL::Crypto
    bcrypt
    ncrypt
    Userenv
  )
elseif(APPLE)
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/librust.a"

    ${WFA2_LIBRARIES}

    "${wfa2_BINARY_DIR}/libwfa2cpp.a"
    "${wfa2_BINARY_DIR}/libwfa2.a"

    -lz
    -lm
    -lbz2
    ${XZ_ROOT_DIR}/lib/liblzma.a
    -lcurl
    ${OPENSSL_LIBRARIES}

    "-framework CoreFoundation"
    "-framework Security"
    "-framework Accelerate"
  )
elseif(UNIX)
  target_link_libraries(${EXTENSION_NAME}
    PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/librust.a"

    "${wfa2_BINARY_DIR}/libwfa2cpp.a"
    "${wfa2_BINARY_DIR}/libwfa2.a"

    -lz
    -lm
    -lbz2
    -llzma
    -lcurl
    ${OPENSSL_LIBRARIES}
  )
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

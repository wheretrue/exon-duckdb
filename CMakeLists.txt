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

  execute_process(COMMAND brew --prefix llvm@15 OUTPUT_VARIABLE LLVM_PREFIX)
  string(STRIP ${LLVM_PREFIX} LLVM_ROOT_DIR)

  # Set the required clang version to 16
  set(CC ${LLVM_ROOT_DIR}/bin/clang)
  set(CXX ${LLVM_ROOT_DIR}/bin/clang++)

  set(CMAKE_C_COMPILER ${CC})
  set(CMAKE_CXX_COMPILER ${CXX})

  message("-- wtt: CC is ${CC}")
  message("-- wtt: CXX is ${CXX}")

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

add_subdirectory(wtt01_rust)

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
  arrow
  GIT_REPOSITORY https://github.com/apache/arrow.git
  GIT_TAG apache-arrow-11.0.0
)

list(APPEND available_contents httplib json arrow)

FetchContent_MakeAvailable(${available_contents})

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

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-constexpr-not-const")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wc++14-extensions")

if(WFA2_ENABLED)
  message("-- wtt: WFA2_ENABLED is ${WFA2_ENABLED}")
  FetchContent_Declare(wfa2
    GIT_REPOSITORY https://github.com/tshauck/WFA2-lib.git
    GIT_TAG 706c9db58f2f5d831cf97a02cdfdda11b1a0c8db
  )

  FetchContent_MakeAvailable(wfa2)
  include_directories(${wfa2_SOURCE_DIR})
endif()

include_directories(src/include)

set(EXTENSION_SOURCES
  src/wtt01_extension.cpp
  src/check_license.cpp

  src/bam_io.cpp
  src/bcf_io.cpp
  src/bed_io.cpp
  src/fasta_io.cpp
  src/fastq_io.cpp
  src/genbank_io.cpp
  src/gff_io.cpp
  src/gtf_io.cpp
  src/hmm_io.cpp
  src/sam_cigar_parse.cpp
  src/sam_io.cpp
  src/sequence_functions.cpp
  src/vcf_io.cpp
  src/wtt01_acknowledgement_table.cpp
  src/wtt01_functions.cpp
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
  "${CMAKE_CURRENT_BINARY_DIR}/wtt01_rust/Release/wtt01_rust.lib"
   OpenSSL::SSL OpenSSL::Crypto bcrypt Userenv)

elseif(APPLE)
target_link_libraries(${EXTENSION_NAME}
  PRIVATE
  "${CMAKE_CURRENT_BINARY_DIR}/wtt01_rust/libwtt01_rust.a"
  "${wfa2_BINARY_DIR}/libwfa2cpp.a"
  "${wfa2_BINARY_DIR}/libwfa2.a"
  "-framework CoreFoundation"
  ${OPENSSL_LIBRARIES}
)

else()
target_link_libraries(${EXTENSION_NAME}
  PUBLIC
  "${CMAKE_CURRENT_BINARY_DIR}/wtt01_rust/libwtt01_rust.a"
  "${wfa2_BINARY_DIR}/libwfa2cpp.a"
  "${wfa2_BINARY_DIR}/libwfa2.a"
  ${OPENSSL_LIBRARIES}
)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

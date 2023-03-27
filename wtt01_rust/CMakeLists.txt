project(wtt01_rust)

include(FetchContent)

FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.3.2
)
list(APPEND available_contents Corrosion)

FetchContent_MakeAvailable(${available_contents})

corrosion_import_crate(MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml
  PROFILE release
)

add_subdirectory(exon)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/exon_extension.cpp
    PARENT_SCOPE)
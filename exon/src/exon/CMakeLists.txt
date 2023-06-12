add_subdirectory(arrow_table_function)
add_subdirectory(sam_functions)
add_subdirectory(sequence_functions)
add_subdirectory(gff_functions)
add_subdirectory(fastq_functions)
add_subdirectory(core)

if(WFA2_ENABLED)
        add_subdirectory(alignment_functions)
endif()

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        PARENT_SCOPE
)

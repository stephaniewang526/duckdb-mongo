# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(mongo
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Load TPC-H extension for benchmarking
duckdb_extension_load(tpch)

# DuckDB 2.0 and main only link the extensions listed with duckdb_extension_statically_link into the CLI (v1.5 links every
# loaded one). test/create-tpch-test-db.sh runs LOAD tpch in the build's CLI.
if(COMMAND duckdb_extension_statically_link)
    duckdb_extension_statically_link(tpch)
endif()

cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME mongo)

# MongoDB C++ driver requires C++17 for std::optional and std::string_view.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(bsoncxx CONFIG REQUIRED)
find_package(mongocxx CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/mongo_extension.cpp
    src/mongo_table_function.cpp
    src/mongo_storage_extension.cpp
    src/mongo_catalog.cpp
    src/mongo_schema_entry.cpp
    src/mongo_instance.cpp
    src/mongo_transaction.cpp
    src/mongo_transaction_manager.cpp
    src/mongo_clear_cache.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link MongoDB C++ driver in both the static library as the loadable extension
# Use static libraries if available, otherwise use shared
target_link_libraries(${EXTENSION_NAME} 
    $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>
    $<IF:$<TARGET_EXISTS:mongo::bsoncxx_static>,mongo::bsoncxx_static,mongo::bsoncxx_shared>
)
target_link_libraries(${LOADABLE_EXTENSION_NAME} 
    $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>
    $<IF:$<TARGET_EXISTS:mongo::bsoncxx_static>,mongo::bsoncxx_static,mongo::bsoncxx_shared>
)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# Add C++ integration test for MongoDB Atlas
# This test is built separately and can be run independently
# Build the integration test executable
# Note: This is always built regardless of ENABLE_UNITTEST_CPP_TESTS since it's an extension-specific test
add_executable(test_atlas_integration
  test/integration/test_atlas_integration.cpp
  ${EXTENSION_SOURCES}
)

# Set output directory - force debug builds to go to build/debug/
# CMAKE_CURRENT_SOURCE_DIR is the directory containing this CMakeLists.txt (project root)
# For debug builds, output to build/debug/ relative to project root
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(DEBUG_OUTPUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build/debug")
  # Ensure the directory exists
  file(MAKE_DIRECTORY "${DEBUG_OUTPUT_DIR}")
  set_target_properties(test_atlas_integration PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${DEBUG_OUTPUT_DIR}"
  )
  message(STATUS "test_atlas_integration (DEBUG) will be built in: ${DEBUG_OUTPUT_DIR}")
else()
  # Release builds can use the default extension location
  set_target_properties(test_atlas_integration PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/extension/mongo"
  )
  message(STATUS "test_atlas_integration (RELEASE) will be built in: ${CMAKE_BINARY_DIR}/extension/mongo")
endif()

# Link against DuckDB and MongoDB drivers
target_link_libraries(test_atlas_integration
  duckdb_static
  $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>
  $<IF:$<TARGET_EXISTS:mongo::bsoncxx_static>,mongo::bsoncxx_static,mongo::bsoncxx_shared>
)

# Include directories
target_include_directories(test_atlas_integration PRIVATE
  src/include
  duckdb/src/include
  duckdb/third_party/catch
  duckdb/test/include
)

# Ensure debug symbols are enabled for debugging (always, regardless of build type)
target_compile_options(test_atlas_integration PRIVATE -g -O0)

# Add test to CTest (optional)
add_test(NAME test_atlas_integration COMMAND test_atlas_integration "[mongo][atlas][integration]")

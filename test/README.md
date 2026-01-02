# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the mongo extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## Setting Up Test MongoDB Database

Many tests require a MongoDB instance with test data. The `make test` command automatically sets `MONGODB_TEST_DATABASE_AVAILABLE=1`, but you still need MongoDB running and test data created.

### Automated Setup (Recommended)

The easiest way to run tests with MongoDB is using the automated script:

```bash
bash test/run-tests-with-mongo.sh
```

This script will:
1. Start a MongoDB Docker container (if not already running)
2. Create test data
3. Run all tests

### Manual Setup

If you want to run `make test` directly, you need to:

1. **Start MongoDB** (if not already running):
   ```bash
   # Using Docker
   docker run -d -p 27017:27017 --name mongodb-test mongo:latest
   
   # Or use an existing MongoDB instance
   ```

2. **Create test data**:
   ```bash
   ./test/create-mongo-tables.sh
   ./test/create-tpch-test-db.sh  # Creates TPC-H test database (sf=0.01)
   ```

3. **Run tests**:
   ```bash
   make test
   ```
   
   The `MONGODB_TEST_DATABASE_AVAILABLE` environment variable is automatically set by the Makefile.

The setup script creates two databases:

1. **`duckdb_mongo_test`** - General test database with the following collections:
   - `users` - Sample user data with various types (strings, numbers, booleans, dates, nested objects, arrays)
   - `products` - Product data with nested specs
   - `orders` - Order data with nested arrays
   - `empty_collection` - Empty collection for edge case testing
   - `type_conflicts` - Collection with type conflicts
   - `deeply_nested` - Collection with deeply nested documents

2. **`tpch_test`** - TPC-H test database (always scale factor 0.01) for unit tests:
   - Contains all 8 TPC-H tables (region, nation, supplier, customer, part, partsupp, orders, lineitem)
   - Always uses scale factor 0.01 (~60K lineitems) to match expected test results
   - Separate from `tpch` database used for benchmarks (which can be any scale factor)

Tests that require MongoDB use `require-env MONGODB_TEST_DATABASE_AVAILABLE` and will be skipped if the environment variable is not set.

## Cleanup Test Files

Tests run from `duckdb_unittest_tempdir/` to contain test database files created by `ATTACH` commands. These files are automatically ignored by git (see `.gitignore`). 

The automated test script (`test/run-tests-with-mongo.sh`) automatically cleans up these files after running tests. You can also clean them manually:

```bash
make cleanup-test-files
```

Or directly:
```bash
bash scripts/cleanup_test_files.sh
```
## C++ Integration Tests

In addition to SQL logic tests, this extension includes C++ integration tests for testing connectivity to real MongoDB instances. These tests verify that the extension works correctly with actual MongoDB deployments.

**Important**: Integration tests verify data from specific MongoDB instances. Each test may expect particular databases, collections, and document structures. If you run tests against a different MongoDB instance, you may need to either set up matching test data or modify the test expectations accordingly.

### Building Integration Tests

Integration tests require C++ unit tests to be enabled. The extension Makefile hardcodes `ENABLE_UNITTEST_CPP_TESTS=FALSE`, so you need to use CMake directly:

```bash
mkdir -p build/release
cd build/release
cmake -DENABLE_UNITTEST_CPP_TESTS=TRUE -GNinja ../..
ninja
cd ../..
```

This will build all integration test executables in `build/release/extension/mongo/`.

### Running Integration Tests

Integration tests are executable files that use the Catch2 test framework. Run them directly:

```bash
./build/release/extension/mongo/test_atlas_integration "[mongo][atlas][integration]"
```

You can filter tests by tags (the part in square brackets). For example:
- `"[mongo][atlas][integration]"` - Run only Atlas integration tests
- `"[mongo][integration]"` - Run all integration tests
- `"[mongo]"` - Run all mongo tests

Many integration tests require environment variables for connection credentials. If required variables are not set, tests will typically skip automatically without failing.

### Adding New Integration Tests

To add a new C++ integration test:

1. **Create a new `.cpp` file** in `test/integration/` (e.g., `test/integration/test_my_integration.cpp`)

2. **Use the Catch2 test framework** (same as DuckDB core tests) with appropriate test tags

3. **Add the test executable to CMakeLists.txt** if needed. Look for existing test executables in `CMakeLists.txt` and follow the same pattern.

4. **Rebuild** the project with `ENABLE_UNITTEST_CPP_TESTS=TRUE` enabled

Example test structure:
```cpp
#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "mongo_extension.hpp"

// Helper macro for checking query success
#define REQUIRE_NO_FAIL(result) REQUIRE(!(result)->HasError())

// Simple main function for running the test
int main(int argc, char* argv[]) {
    return Catch::Session().run(argc, argv);
}

TEST_CASE("My Integration Test", "[mongo][integration]") {
    // Load the extension
    duckdb::DuckDB db(nullptr);
    db.LoadStaticExtension<duckdb::MongoExtension>();
    duckdb::Connection con(db);
    
    // Your test code here
    // Use REQUIRE_NO_FAIL() for queries that should succeed
    // Use REQUIRE() for assertions
}
```

**Test tags**: Use tags like `[mongo][integration]` or `[mongo][atlas][integration]` to allow filtering when running tests. Tests that require specific credentials should check for environment variables and skip gracefully if they're not available.

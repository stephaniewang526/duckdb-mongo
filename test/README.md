# Testing this extension
This directory contains all tests for this extension. Write tests in the `sql` directory using [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) format. Prefer SQL-based tests over C++ tests when possible.

The root Makefile contains targets to build and run all tests. To run SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## Setting Up Test MongoDB Database

Many tests require a MongoDB instance with test data. The `make test` command automatically sets `MONGODB_TEST_DATABASE_AVAILABLE=1`, but MongoDB must be running with test data created.

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

1. **`duckdb_mongo_test`** - General test database with collections: `users`, `products`, `orders`, `empty_collection`, `type_conflicts`, `deeply_nested`

2. **`tpch_test`** - TPC-H test database (scale factor 0.01) with all 8 TPC-H tables. Separate from the `tpch` database used for benchmarks.

Tests that require MongoDB use `require-env MONGODB_TEST_DATABASE_AVAILABLE` and will be skipped if the environment variable is not set.

## Cleanup Test Files

Tests run from `duckdb_unittest_tempdir/` to contain test database files created by `ATTACH` commands. These files are automatically ignored by git (see `.gitignore`). 

The automated test script (`test/run-tests-with-mongo.sh`) automatically cleans up these files after running tests. To clean them manually, remove the `duckdb_unittest_tempdir/` directory.

## C++ Integration Tests

In addition to SQL logic tests, this extension includes C++ integration tests for testing connectivity to real MongoDB instances. These tests verify that the extension works correctly with actual MongoDB deployments.

**Important**: Integration tests verify data from specific MongoDB instances. Each test may expect particular databases, collections, and document structures. If you run tests against a different MongoDB instance, you may need to either set up matching test data or modify the test expectations accordingly.

### Building Integration Tests

Build integration tests using CMake:

```bash
mkdir -p build/release
cd build/release
cmake -GNinja ../..
ninja test_atlas_integration
cd ../..
```

This builds the integration test executable in `build/release/extension/mongo/`. Note: `test_atlas_integration` is always built regardless of `ENABLE_UNITTEST_CPP_TESTS`.

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

3. **Add the test executable to the root `CMakeLists.txt`**. Look for `test_atlas_integration` as an example and follow the same pattern.

4. **Rebuild** the project using the build instructions above

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

**Test tags**: Use tags like `[mongo][integration]` or `[mongo][atlas][integration]` to allow filtering. Tests requiring credentials should check for environment variables and skip gracefully if unavailable.

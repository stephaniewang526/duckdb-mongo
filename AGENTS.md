# duckdb-mongo Agent Guidelines

This file provides guidance to AI coding agents working with code in this repository.

## What This Repo Is

A DuckDB extension that integrates MongoDB, enabling direct SQL queries over MongoDB collections without ETL. It uses the MongoDB C++ driver (via vcpkg) and extends DuckDB with a custom catalog, storage extension, and optimizer for query pushdown.

## Agent Skills

Reusable workflows in `.agents/skills/`. When a task matches one of these, read the corresponding file and follow its steps:

- **`.agents/skills/test.md`** — Use when asked to build, run tests, or verify changes. Covers MongoDB setup, incremental builds, and test execution.
- **`.agents/skills/repro-issue.md`** — Use when asked to investigate, reproduce, or fix a GitHub issue. Covers fetching the issue, root cause analysis, writing a failing test, and verifying the fix.
- **`.agents/skills/bump-duckdb-version.md`** — Use when asked to update the DuckDB submodule version. Covers updating submodules, fixing compilation, updating CI/CD and README, and running tests.

## Development Command Quick Reference

### Build Commands

- `make release` — Build the extension (release mode, output in `build/release/`)
- `make debug` — Build in debug mode
- `make reldebug` — Release build with debug symbols
- `cmake --build build/release --target mongo_loadable_extension` — Rebuild just the loadable extension
- `cmake --build build/release --target unittest` — Rebuild just the test binary

The build produces:
- `build/release/extension/mongo/mongo.duckdb_extension` — Loadable extension
- `build/release/test/unittest` — Test runner (embeds DuckDB + extension)

There is **no standalone DuckDB CLI binary** in this build. To interact with the extension manually, use a system-installed `duckdb` with `LOAD 'build/release/extension/mongo/mongo.duckdb_extension';`.

### Build Prerequisites

- CMake, C++17 compiler
- vcpkg with `mongo-cxx-driver` (run `bash scripts/setup-vcpkg.sh` if not set up)
- Set `VCPKG_TOOLCHAIN_PATH` if vcpkg isn't auto-detected (see `scripts/check-vcpkg.sh`)

### Testing Commands

**Tests require a running MongoDB instance with test data.** The full automated workflow:

```bash
bash test/run-tests-with-mongo.sh   # Starts Docker MongoDB, creates data, builds, runs tests
```

To run tests manually (when MongoDB is already running on localhost:27017):

```bash
# 1. Create test data (only needed once per MongoDB instance)
bash test/create-mongo-tables.sh
bash test/create-tpch-test-db.sh    # Optional: for TPC-H tests

# 2. Run all tests
MONGODB_TEST_DATABASE_AVAILABLE=1 make test_release

# 3. Run a single test file
MONGODB_TEST_DATABASE_AVAILABLE=1 build/release/test/unittest "test/sql/schema/schema.test"
```

**Without MongoDB**, all tests requiring `require-env MONGODB_TEST_DATABASE_AVAILABLE` will skip (which is most of them).

### Formatting

Code style is inherited from DuckDB (`.clang-format` symlinks to `duckdb/.clang-format`):
- LLVM-based style, 120-char line limit, tab indentation
- Run: `clang-format -i src/*.cpp src/**/*.cpp src/include/*.hpp`

## Code Architecture

### Source Organization

```
src/
├── include/                        # Header files
│   ├── mongo_table_function.hpp    # Core data structures (MongoScanData, MongoScanState)
│   ├── mongo_catalog.hpp           # MongoCatalog, MongoDefaultGenerator
│   ├── mongo_schema_entry.hpp      # Schema catalog entry
│   ├── mongo_instance.hpp          # MongoDB client singleton
│   ├── mongo_filter_pushdown.hpp   # Filter pushdown API
│   ├── mongo_expr_pushdown.hpp     # Expression pushdown API
│   ├── mongo_optimizer.hpp         # Custom optimizer
│   ├── mongo_secrets.hpp           # Secrets integration
│   └── ...
├── schema/
│   ├── mongo_schema_inference_helpers.cpp   # BSON→DuckDB type conversion, JSON normalization
│   └── mongo_schema_inference_internal.hpp  # Internal schema inference API
├── mongo_extension.cpp             # Extension entry point (registers everything)
├── mongo_table_function.cpp        # mongo_scan bind/init/execute
├── mongo_schema_inference.cpp      # Schema inference from documents
├── mongo_catalog.cpp               # Catalog (databases→schemas, collections→views)
├── mongo_optimizer.cpp             # Query pushdown optimizer
├── mongo_filter_pushdown.cpp       # WHERE clause → MongoDB query translation
├── mongo_expr_pushdown.cpp         # Complex expression pushdown
├── mongo_storage_extension.cpp     # ATTACH support
├── mongo_schema_entry.cpp          # Schema entry management
├── mongo_transaction.cpp           # Read-only transaction support
├── mongo_secrets.cpp               # CREATE SECRET support
└── mongo_clear_cache.cpp           # Cache invalidation
```

### Key Concepts

- **ATTACH flow**: `mongo_storage_extension.cpp` → creates `MongoCatalog` → `MongoDefaultGenerator` creates views backed by `mongo_scan` table function
- **Schema inference**: Samples documents (default 100), flattens nested docs with `_` separator (e.g., `address.city` → `address_city`), parent docs also exposed as VARCHAR/JSON columns
- **Query pushdown**: Custom optimizer (`mongo_optimizer.cpp`) rewrites DuckDB plans to push filters, projections, limits, and aggregations to MongoDB
- **Filter pushdown**: Translates DuckDB `TableFilterSet` to MongoDB `$match` queries; handles ObjectId detection, type coercion, and complex expressions

### Test Organization

```
test/
├── sql/
│   ├── mongo.test                  # Basic functionality
│   ├── attach/                     # ATTACH operations
│   ├── query/                      # Filters, pushdown, aggregations
│   ├── schema/                     # Schema inference tests
│   ├── secrets/                    # Secrets management
│   ├── cache/                      # Cache clearing
│   ├── edge_cases/                 # Empty collections
│   └── tpch/                       # TPC-H benchmark queries
├── create-mongo-tables.sh          # Creates duckdb_mongo_test database
├── create-tpch-test-db.sh          # Creates tpch_test database (sf=0.01)
├── run-tests-with-mongo.sh         # Full automated test workflow
└── unit/                           # C++ unit tests
```

Tests use DuckDB's SQLLogicTest format. Most tests require `require-env MONGODB_TEST_DATABASE_AVAILABLE`.

### Test Data

Two MongoDB databases are used:
- `duckdb_mongo_test` — Collections: users, products, orders, matrix, decimal_test, type_conflicts, deeply_nested, nested_scalars_test, object_container_test, string_id_test, schema_test_* (created by `create-mongo-tables.sh`)
- `tpch_test` — TPC-H tables at scale factor 0.01 (created by `create-tpch-test-db.sh`)

## Important Patterns

### Adding a New Column/Feature to Schema Inference

1. Modify `CollectFieldPaths()` in `mongo_schema_inference.cpp` to detect the new pattern
2. If needed, add type handling in `FlattenDocument()` (same file) for data reading
3. Add test data to `test/create-mongo-tables.sh`
4. Add SQLLogicTest cases to appropriate file in `test/sql/schema/`
5. Update `test/sql/attach/catalog_operations.test` if column layout changes (DESCRIBE output)

### Adding Filter Pushdown Support

1. Add translation logic in `mongo_filter_pushdown.cpp` (for simple filters) or `mongo_expr_pushdown.cpp` (for expressions)
2. Add tests in `test/sql/query/` (both correctness and EXPLAIN plan verification)

### SQLLogicTest Format

```sql
# Comment
require mongo
require-env MONGODB_TEST_DATABASE_AVAILABLE

statement ok
ATTACH 'host=localhost port=27017 dbname=duckdb_mongo_test' AS mongo_test (TYPE MONGO);

query III    # I=integer, T=boolean, other type chars: see DuckDB docs
SELECT col1, col2, col3 FROM mongo_test.collection WHERE condition;
----
expected_col1	expected_col2	expected_col3

statement ok
DETACH mongo_test;
```

## CI/CD

Two GitHub Actions workflows in `.github/workflows/`:

- **MongoDBTests.yml** — Integration tests with real MongoDB 7.0 (Docker service). Builds, creates test data, runs `make test_release`, verifies ≥13 tests pass with no skips.
- **MainDistributionPipeline.yml** — Extension distribution builds using DuckDB's `extension-ci-tools`. Builds for multiple platforms, runs format checks.

## Common Pitfalls

- **Stale build**: The `make release` repository packaging step can fail with `FileExistsError`. Fix: `rm -rf build/release/repository` and rebuild.
- **Test failures without MongoDB**: All integration tests skip. This is expected — they need `MONGODB_TEST_DATABASE_AVAILABLE=1` and a running MongoDB.
- **JSON format in tests**: Don't assert exact JSON strings from `bsoncxx::to_json()` — spacing varies by library version. Use `LIKE '%expected_substring%'` or `IS NOT NULL` checks instead.
- **Underscore flattening**: Nested doc `a.b` becomes column `a_b`. If a top-level field `a_b` also exists, there's a name collision. Be aware of this when working with schema inference.
- **DuckDB submodule**: The `duckdb` directory is a git submodule pinned to a specific version. Don't update it without validating compatibility.


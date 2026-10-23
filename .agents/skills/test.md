# Build and Test

Rebuild the extension and run tests against a live MongoDB instance.

## Prerequisites

1. **Ensure MongoDB is running**: `docker start mongodb-test` (or `docker run -d -p 27017:27017 --name mongodb-test mongo:latest` if no container exists). Verify with `docker exec mongodb-test mongosh --eval "db.adminCommand('ping')"`.

2. **Ensure test data exists**: Check `docker exec mongodb-test mongosh --quiet --eval "db.getSiblingDB('duckdb_mongo_test').getCollectionNames().length"`. If 0, run `bash test/create-mongo-tables.sh`.

## Build

Run an incremental build:

```bash
cmake --build build/release --target unittest
```

If the build fails due to stale artifacts, run `rm -rf build/release/repository` and retry.

## Run Tests

- **All tests**: `MONGODB_TEST_DATABASE_AVAILABLE=1 build/release/test/unittest`
- **As CI runs them**: `MONGODB_TEST_DATABASE_AVAILABLE=1 make test_release` (files run in parallel through `duckdb/scripts/ci/run_tests.py`, so write tests may overlap with listing tests)
- **Single file**: `MONGODB_TEST_DATABASE_AVAILABLE=1 build/release/test/unittest "test/sql/schema/schema.test"`
- **By keyword**: find matching files with `find test/sql -name "*.test" -path "*<keyword>*"` and run each match

## Interpreting Results

- If tests fail due to missing collections, connectivity errors, or skipped tests, revisit the prerequisites above.
- Show the pass/fail summary line and, for failures, the query that failed with actual vs expected output.

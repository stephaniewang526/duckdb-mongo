# duckdb-mongo

Integrates DuckDB with MongoDB, enabling direct SQL queries over MongoDB collections without exporting data or ETL.

## Overview

The `duckdb-mongo` extension allows you to query MongoDB collections directly from DuckDB using SQL. It provides a table function `mongo_scan` that connects to MongoDB, infers the schema from your documents, and returns the data as a DuckDB table.

## Features

- **Direct MongoDB Queries**: Query MongoDB collections directly from DuckDB without exporting data
- **Automatic Schema Inference**: Automatically infers schema from MongoDB documents (samples first 100 documents by default)
- **Nested Document Support**: Handles nested documents by flattening them with underscore-separated column names
- **Type Mapping**: Maps MongoDB BSON types to DuckDB SQL types
- **Filter Support**: Optional MongoDB query filter to limit results
- **Read-Only**: Currently supports read-only queries (write support may be added in the future)

## Building

### Prerequisites

- CMake 3.5 or higher
- C++ compiler with C++17 support
- vcpkg (for dependency management)

### Managing dependencies

DuckDB extensions use VCPKG for dependency management. To set up VCPKG:

```shell
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh  # On Windows: .\bootstrap-vcpkg.bat
export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake
```

### Build steps

1. Clone the repository with submodules:
```sh
git clone --recurse-submodules https://github.com/stephaniewang526/duckdb-mongo.git
cd duckdb-mongo
```

2. Install dependencies (first time only):
```sh
# Install MongoDB C++ driver via vcpkg
../vcpkg/vcpkg install --triplet arm64-osx  # or x64-osx for Intel Mac
```

3. Build the extension:
```sh
# Set vcpkg environment
export VCPKG_TOOLCHAIN_PATH=../vcpkg/scripts/buildsystems/vcpkg.cmake
export VCPKG_TARGET_TRIPLET=arm64-osx  # or x64-osx for Intel Mac

# Build
make release
```

Or use the build script:
```sh
bash scripts/build.sh
```

The main binaries that will be built are:
- `./build/release/duckdb` - DuckDB shell with the extension pre-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/mongo/mongo.duckdb_extension` - Loadable extension binary

## Usage

### Loading the Extension

If you built the extension locally, start DuckDB with:
```sh
./build/release/duckdb
```

The extension is automatically loaded. For distributed binaries, load it explicitly:
```sql
LOAD '/path/to/mongo.duckdb_extension';
```

### Attaching MongoDB Databases

You can attach a MongoDB database using the `ATTACH` command. The extension supports two connection string formats:

#### Key-Value Format (Recommended)

Similar to Postgres libpq format, use space-separated key=value pairs:

```sql
-- Basic connection
ATTACH 'host=localhost port=27017' AS mongo_db (TYPE MONGO);

-- With authentication
ATTACH 'host=localhost port=27017 user=myuser password=mypass' AS mongo_db (TYPE MONGO);

-- With database name
ATTACH 'host=localhost port=27017 dbname=mydb' AS mongo_db (TYPE MONGO);

-- With authentication source
ATTACH 'host=localhost port=27017 user=myuser password=mypass authsource=admin' AS mongo_db (TYPE MONGO);
```

**Supported parameters:**
- `host` - MongoDB host (default: `localhost`)
- `port` - MongoDB port (default: `27017`)
- `dbname` - Database name (optional)
- `user` - Username for authentication (optional)
- `password` - Password for authentication (optional)
- `authsource` - Authentication database (optional, default: `admin`)

#### MongoDB URI Format

You can also use the standard MongoDB URI format, but you must use `TYPE MONGO` explicitly:

```sql
ATTACH 'mongodb://localhost:27017' AS mongo_db (TYPE MONGO);
ATTACH 'mongodb://user:pass@localhost:27017/mydb' AS mongo_db (TYPE MONGO);
ATTACH 'mongodb+srv://cluster.mongodb.net' AS mongo_db (TYPE MONGO);
```

**Note:** The key=value format is recommended because it avoids issues with DuckDB trying to open `mongodb://` URLs as files.

Once attached, you can query MongoDB databases and collections:

```sql
-- List databases (shown as schemas)
SHOW SCHEMAS;

-- List collections in a database (shown as tables/views)
SHOW TABLES FROM mydb;

-- Query a collection
SELECT * FROM mydb.mycollection;
```

### Querying MongoDB Collections

You can also use the `mongo_scan` table function directly to query MongoDB collections:

```sql
-- Basic usage: connection_string, database, collection
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection');

-- With optional filter (MongoDB query as JSON string)
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017',
    'mydb',
    'mycollection',
    filter := '{"status": "active"}'
);

-- With custom sample size for schema inference
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017',
    'mydb',
    'mycollection',
    sample_size := 500
);

-- Combine filter and sample size
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017',
    'mydb',
    'mycollection',
    filter := '{"status": "active"}',
    sample_size := 200
);
```

### Schema Inference

The extension automatically infers the schema by sampling documents from the collection (default: 100 documents). It:

- Samples the first N documents (configurable via `sample_size` parameter)
- Collects all unique field paths across sampled documents
- Flattens nested documents using underscore-separated column names (e.g., `user_name`, `user_address_city`)
- Handles missing fields by allowing NULL values
- Maps MongoDB BSON types to DuckDB SQL types (see [Schema Inference Details](#schema-inference-details) below)
- Resolves type conflicts using frequency-based heuristics (prefers numeric types when appropriate)

### Example Queries

```sql
-- Count documents
SELECT COUNT(*) FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection');

-- Filter and aggregate
SELECT status, COUNT(*) as count
FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection')
WHERE status = 'active'
GROUP BY status;

-- Join with other DuckDB tables
SELECT m.*, t.description
FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection') m
JOIN transactions t ON m.id = t.mongo_id;
```

## Schema Inference Details

The extension uses a sophisticated schema inference algorithm inspired by MongoDB's internal schema inference proposal.

### Type Mapping

MongoDB BSON types are mapped to DuckDB SQL types as follows:
- String → VARCHAR
- Number (int32/int64) → BIGINT
- Number (double) → DOUBLE
- Boolean → BOOLEAN
- Date → TIMESTAMP
- ObjectId → VARCHAR
- Arrays → JSON (normalized to compact format)
- Nested Documents → JSON (if depth > 5) or flattened

### Type Conflict Resolution

When a field has mixed types across documents, the extension uses frequency-based resolution:
- If VARCHAR is a strong majority (>70%), prefers VARCHAR (most flexible for truly mixed data)
- If numeric types (DOUBLE/BIGINT) represent ≥30% of values, prefers the numeric type
- DOUBLE is preferred over BIGINT when both are present (can represent integers)
- Boolean and Timestamp require strong majority (≥70%) to be selected
- Otherwise defaults to VARCHAR for maximum compatibility

### Other Details

- **Key Path Methodology**: Uses dot notation internally, converts to underscore-separated column names
- **Nested Documents**: Flattens up to 5 levels deep, stores deeper structures as JSON
- **Array Handling**: Stores arrays as JSON (no positional information retained), normalized to compact format

## Limitations

- **Read-Only**: Currently supports read-only queries
- **Schema Sampling**: Schema is inferred from a sample of documents, may miss fields in unsampled documents
- **Performance**: Schema inference happens on every query (future versions may cache schemas)

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

See LICENSE file for details.

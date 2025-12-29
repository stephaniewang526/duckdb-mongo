# duckdb-mongo

Integrates DuckDB with MongoDB, enabling direct SQL queries over MongoDB collections without exporting data or ETL.

## Overview

The `duckdb-mongo` extension allows you to query MongoDB collections directly from DuckDB using SQL. It provides a table function `mongo_scan` that connects to MongoDB, infers the schema from your documents, and returns the data as a DuckDB table.

## Architecture

The extension provides **direct SQL access to MongoDB without exporting or copying data**. All queries execute against live MongoDB data in real-time.

```
┌──────────────────┐
│ User/Application │
│   (SQL Queries)  │
└────────┬─────────┘
         │ SQL Query
         ▼
┌─────────────────────────────────────────┐
│         DUCKDB ENGINE                   │
│  ┌───────────────────────────────────┐  │
│  │ SQL Planning & Optimization       │  │
│  │ - Query planning                  │  │
│  │ - Filter pushdown analysis        │  │
│  └───────────────────────────────────┘  │
│  ┌───────────────────────────────────┐  │
│  │ SQL Execution (Analytical)        │  │
│  │ - Joins                           │  │
│  │ - Aggregations (GROUP BY, COUNT)  │  │
│  │ - Sorting                         │  │
│  │ - Window functions                │  │
│  │ - Complex SQL operations          │  │
│  └───────────────────────────────────┘  │
└────────┬────────────────────────────────┘
         │ Table Function Call (mongo_scan)
         │ Requests data chunks
         ▼
┌─────────────────────────────────────────┐
│ duckdb-mongo Extension                  │
│                                         │
│  • Schema Inference                     │
│  • Filter Translation                   │
│  • BSON → Columnar                      │
│  • Type Conversion                      │
└────────┬────────────────────────────────┘
         │
         │ MongoDB Query (filtered)
         │ Stream documents on-demand
         ▼
┌─────────────────────────────────────────┐
│         MONGODB DATABASE                │
│  ┌───────────────────────────────────┐  │
│  │ Document Store Operations         │  │
│  │ - Index lookups                   │  │
│  │ - Document filtering              │  │
│  │ - Cursor management               │  │
│  │ - Document retrieval              │  │
│  └───────────────────────────────────┘  │
│                                         │
│  Data stays here (No ETL/Export)        │
└─────────────────────────────────────────┘
```

### Execution Responsibilities

**MongoDB Handles:**
- **Document filtering**: Uses MongoDB indexes and query engine to filter documents efficiently
- **Index lookups**: Leverages MongoDB's B-tree indexes for fast document retrieval
- **Cursor management**: Manages result sets and pagination
- **Document storage**: Data remains in MongoDB's native BSON format

MongoDB is optimized for document storage and retrieval. By pushing filters down to MongoDB, we leverage its indexing capabilities and reduce the amount of data transferred over the network.

**DuckDB Handles:**
- **SQL planning**: Analyzes the SQL query and creates an execution plan
- **Analytical operations**: Performs joins, aggregations, sorting, window functions, and complex SQL transformations
- **Columnar processing**: Executes operations on columnar in-memory data structures
- **Query optimization**: Applies DuckDB's query optimizer for analytical workloads

DuckDB is optimized for analytical SQL queries on columnar data. It excels at aggregations, joins, and complex analytical operations that MongoDB's document model isn't designed for.

### Storage vs Compute Separation

This extension implements a **separation of storage from compute** architecture:

**Storage Layer (MongoDB):**
- **Format**: BSON (Binary JSON) - document-oriented storage
- **Location**: Persistent storage in MongoDB
- **Optimization**: Optimized for document writes, reads, and indexing
- **Data Model**: Flexible schema with nested documents and arrays

**Compute Layer (DuckDB):**
- **Format**: Columnar in-memory format
- **Location**: DuckDB's memory buffers (temporary, query-scoped)
- **Optimization**: Optimized for analytical queries (scans, aggregations, joins)
- **Data Model**: Structured columns with inferred types

**Why This Separation?**

1. **Leverage Best of Both Worlds**: MongoDB excels at document storage and operational queries, while DuckDB excels at analytical SQL workloads. Each system operates in its optimal format.

2. **No Data Duplication**: Data remains in MongoDB. The columnar format is created on-the-fly in memory only for query execution, then discarded. No persistent copies or ETL pipelines needed.

3. **Format Conversion**: The extension bridges the gap by converting BSON documents → columnar format only when needed for analytical processing. This conversion happens incrementally as data streams from MongoDB.

4. **Efficient Resource Usage**: MongoDB handles storage, indexing, and document filtering. DuckDB handles analytical computation on columnar data. Each system does what it's best at.

5. **Real-Time Analytics**: Since conversion happens on-demand during query execution, you always query the latest data without maintaining separate analytical databases or data warehouses.

### Data Flow

1. **SQL Planning**: DuckDB receives SQL queries and plans execution. When it encounters `mongo_scan()` or attached MongoDB tables, it calls the extension.

2. **Schema Inference**: The extension samples documents from MongoDB to infer column names and types, creating a DuckDB table schema.

3. **Filter Pushdown**: Filters from SQL WHERE clauses are translated to MongoDB query filters and pushed down to MongoDB. **MongoDB executes these filters** using its indexes and query engine, reducing data transfer.

4. **Document Streaming**: MongoDB returns matching documents via a cursor. Documents are fetched on-demand (streamed) as DuckDB requests more data. No bulk export occurs - data flows incrementally.

5. **BSON to Columnar Conversion**: The extension parses BSON documents and converts them to DuckDB's columnar in-memory format:
   - BSON fields → DuckDB columns
   - Nested documents → flattened columns (e.g., `user.name` → `user_name`)
   - Type conversion (BSON types → DuckDB SQL types)
   - Data written to DuckDB's memory buffers

6. **SQL Execution**: **DuckDB executes** joins, aggregations, sorting, and other SQL operations on the in-memory columnar data, leveraging its analytical query engine.

**Key Points:**
- **No ETL Required**: Data never leaves MongoDB. Queries execute directly against your MongoDB collections.
- **On-the-Fly Schema Inference**: The extension samples documents to infer schema structure and types without pre-processing.
- **Direct Connection**: Uses MongoDB C++ driver to establish native connections to MongoDB instances.
- **Query Translation**: SQL queries are translated to MongoDB queries, with filtering pushed down to MongoDB when possible.
- **Real-Time Results**: All data is fetched and converted on-demand during query execution.

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

#### Key-value format (recommended)

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
- `srv` - Use SRV DNS resolution for MongoDB Atlas (optional, set to `true` for Atlas connections)
- `options` - Additional connection options (optional, e.g., `tls=true&tlsAllowInvalidCertificates=true`)

#### MongoDB Atlas

To connect to MongoDB Atlas hosted clusters, use the `srv=true` parameter:

```sql
-- MongoDB Atlas connection
ATTACH 'host=cluster0.xxxxx.mongodb.net user=myuser password=mypass srv=true' AS atlas_db (TYPE MONGO);

-- With specific database
ATTACH 'host=cluster0.xxxxx.mongodb.net user=myuser password=mypass dbname=mydb srv=true' AS atlas_db (TYPE MONGO);

-- With additional options
ATTACH 'host=cluster0.xxxxx.mongodb.net user=myuser password=mypass srv=true options=authSource=admin' AS atlas_db (TYPE MONGO);
```

When `srv=true` is set, the extension automatically:
- Uses `mongodb+srv://` connection scheme with DNS SRV resolution
- Omits the port (DNS handles service discovery)
- Adds `retryWrites=true&w=majority` for Atlas best practices

**Finding your Atlas connection details:**
1. Go to your MongoDB Atlas cluster
2. Click "Connect" and then "Drivers"
3. Copy the hostname (e.g., `cluster0.xxxxx.mongodb.net`)
4. Use your database user credentials (not your Atlas account)

#### MongoDB URI format

You can also use the standard MongoDB URI format directly:

```sql
ATTACH 'mongodb://localhost:27017' AS mongo_db (TYPE MONGO);
ATTACH 'mongodb://user:pass@localhost:27017/mydb' AS mongo_db (TYPE MONGO);
ATTACH 'mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/mydb?retryWrites=true&w=majority' AS atlas_db (TYPE MONGO);
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

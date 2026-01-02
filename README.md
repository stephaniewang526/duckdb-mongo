# duckdb-mongo

Integrates DuckDB with MongoDB, enabling direct SQL queries over MongoDB collections without exporting data or ETL.

## Quick Start

```sql
-- Attach to MongoDB
ATTACH 'host=localhost port=27017' AS mongo_db (TYPE MONGO);

-- Query your collections
SELECT * FROM mongo_db.mydb.mycollection LIMIT 10;
```

**Using Secrets (recommended for production):**

```sql
-- Create a secret with MongoDB credentials
CREATE SECRET mongo_creds (
    TYPE mongo,
    HOST 'cluster0.xxxxx.mongodb.net',
    USER 'myuser',
    PASSWORD 'mypassword',
    SRV 'true'
);

-- Attach using the secret
ATTACH 'dbname=mydb' AS atlas_db (TYPE mongo, SECRET 'mongo_creds');

-- Query your collections
SELECT * FROM atlas_db.mydb.mycollection;
```

## Installation

### From Source

**Prerequisites:**
- CMake 3.5 or higher
- C++ compiler with C++17 support
- vcpkg (for dependency management)

**Build Steps:**

1. Clone the repository with submodules:
```sh
git clone --recurse-submodules https://github.com/stephaniewang526/duckdb-mongo.git
cd duckdb-mongo
```

2. Set up vcpkg (if not already done):
```shell
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh  # On Windows: .\bootstrap-vcpkg.bat
export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake
```

3. Install dependencies (first time only):
```sh
# Install MongoDB C++ driver via vcpkg
../vcpkg/vcpkg install --triplet arm64-osx  # or x64-osx for Intel Mac
```

4. Build the extension:
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

**Built binaries:**
- `./build/release/duckdb` - DuckDB shell with the extension pre-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/mongo/mongo.duckdb_extension` - Loadable extension binary

### Loading the Extension

```sh
./build/release/duckdb  # Extension auto-loaded
```

Or load explicitly:
```sql
LOAD '/path/to/mongo.duckdb_extension';
```

## Connecting to MongoDB

### Connection String Format

**Key-value format:**
```sql
ATTACH 'host=localhost port=27017' AS mongo_db (TYPE MONGO);
ATTACH 'host=localhost port=27017 dbname=mydb' AS mongo_db (TYPE MONGO);
ATTACH 'host=cluster0.xxxxx.mongodb.net user=myuser password=mypass srv=true' AS atlas_db (TYPE MONGO);
```

**MongoDB URI format:**
```sql
ATTACH 'mongodb://user:pass@localhost:27017/mydb' AS mongo_db (TYPE MONGO);
```

**Connection Parameters:**

| Name | Description | Default | Applies To |
|------|-------------|---------|------------|
| `host` | MongoDB hostname or IP address | `localhost` | Both |
| `port` | MongoDB port number | `27017` | Both |
| `user` / `username` | MongoDB username | - | Both |
| `password` | MongoDB password | - | Both |
| `dbname` / `database` | Specific MongoDB database to connect to | - | Both |
| `authsource` | Authentication database | - | Both |
| `srv` | Use SRV connection format (for MongoDB Atlas) | `false` | Both |
| `options` | Additional MongoDB connection string query parameters | - | Connection string only |

### Using DuckDB Secrets

Store credentials securely using DuckDB Secrets instead of embedding them in connection strings:

```sql
-- Create a secret with MongoDB credentials
CREATE SECRET mongo_creds (
    TYPE mongo,
    HOST 'cluster0.xxxxx.mongodb.net',
    USER 'myuser',
    PASSWORD 'mypassword',
    SRV 'true'
);

-- Attach using the secret (options in ATTACH path merge with secret)
ATTACH 'dbname=mydb' AS atlas_db (TYPE mongo, SECRET 'mongo_creds');
```

**Default secret:** Create an unnamed secret to use as the default for all ATTACH operations:
```sql
CREATE SECRET (TYPE mongo, HOST 'localhost', USER 'myuser', PASSWORD 'mypass');
ATTACH '' AS mongo_db (TYPE mongo);  -- Uses __default_mongo automatically
ATTACH 'dbname=mydb' AS mongo_db (TYPE mongo);  -- Options merge with secret
```

**Note:** An explicit database alias (`AS alias_name`) is required. The `dbname` parameter specifies which MongoDB database to connect to, not the DuckDB database name.

### Entity Mapping

When using `ATTACH` to connect to MongoDB, the extension maps MongoDB entities to DuckDB entities as follows:

```
MongoDB Entity          →  DuckDB Entity
─────────────────────────────────────────
MongoDB Instance        →  Catalog (via ATTACH)
MongoDB Database        →  Schema
MongoDB Collection      →  Table/View
```

**Default Schema Behavior:**

- **Without `dbname`**: Scans all databases, defaults to `"main"` schema
- **With `dbname`**: Uses database name as default schema

```sql
ATTACH 'host=localhost port=27017' AS mongo_all (TYPE MONGO);
USE mongo_all;  -- Defaults to "main"

ATTACH 'host=localhost port=27017 dbname=mydb' AS mongo_db (TYPE MONGO);
USE mongo_db;  -- Defaults to "mydb"
```

## Querying MongoDB

### Basic Queries

```sql
-- Attach to MongoDB
ATTACH 'host=localhost port=27017' AS mongo_db (TYPE MONGO);

-- Show attached databases
SHOW DATABASES;
┌───────────────┐
│ database_name │
│    varchar    │
├───────────────┤
│ memory        │
│ mongo_db      │
└───────────────┘

-- List schemas (MongoDB databases) in the attached catalog
SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'mongo_db' ORDER BY schema_name;
┌───────────────────┐
│    schema_name    │
│      varchar      │
├───────────────────┤
│ duckdb_mongo_test │
│ main              │
│ tpch              │
│ tpch_test         │
└───────────────────┘

-- Select data from a specific collection
SELECT order_id, status, total FROM mongo_db.duckdb_mongo_test.orders;
┌──────────┬───────────┬─────────┐
│ order_id │  status   │  total  │
│ varchar  │  varchar  │ double  │
├──────────┼───────────┼─────────┤
│ ORD-001  │ completed │ 1059.97 │
│ ORD-002  │ pending   │  299.99 │
└──────────┴───────────┴─────────┘

-- Query with aggregation
SELECT status, COUNT(*) as count, SUM(total) as total_revenue 
FROM mongo_db.duckdb_mongo_test.orders 
GROUP BY status;
┌───────────┬───────┬───────────────┐
│  status   │ count │ total_revenue │
│  varchar  │ int64 │    double     │
├───────────┼───────┼───────────────┤
│ pending   │     1 │        299.99 │
│ completed │     1 │       1059.97 │
└───────────┴───────┴───────────────┘
```

### Using mongo_scan Directly

You can also use the `mongo_scan` table function directly without attaching:

```sql
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection');
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection', 
                         filter := '{"status": "active"}', sample_size := 200);
```

## Reference

### BSON Type Mapping

| BSON Type | DuckDB Logical Type | Notes |
|-----------|---------------------|-------|
| `String` | `VARCHAR` | |
| `Int32`, `Int64` | `BIGINT` | |
| `Double` | `DOUBLE` | |
| `Boolean` | `BOOLEAN` | |
| `Date` | `TIMESTAMP` / `DATE` | `DATE` if time component is midnight UTC, else `TIMESTAMP` |
| `ObjectId` | `VARCHAR` | |
| `Binary` | `BLOB` | |
| `Array` | `VARCHAR` | Stored as JSON string |
| `Document` | `VARCHAR` | Stored as JSON string |
| Other | `VARCHAR` | Default for unknown types |

### Schema Inference

The extension automatically infers schemas by sampling documents (default: 100, configurable via `sample_size`):

- **Nested Documents**: Flattened with underscore-separated names (e.g., `user_address_city`), up to 5 levels deep
- **Type Conflicts**: Frequency-based resolution (VARCHAR if >70%, numeric if ≥30%, defaults to VARCHAR)
- **Missing Fields**: NULL values

### Features

- Direct SQL queries over MongoDB collections (no ETL/export)
- Automatic schema inference (samples 100 documents by default)
- Nested document flattening with underscore-separated names
- BSON type mapping to DuckDB SQL types
- **Filter pushdown**: WHERE clauses pushed to MongoDB to leverage indexes
- Optional MongoDB query filters
- Read-only (write support may be added)

### Limitations

- Read-only
- Schema inferred from sample (may miss fields)
- Schema re-inferred per query

## Advanced Topics

### Architecture

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

### mongo_scan Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    mongo_scan Execution                         │
└─────────────────────────────────────────────────────────────────┘

1. BIND PHASE (happens once per query)
   ┌────────────────────────────────────────────────────────────┐
   │ Parse connection string, database, collection              │
   │ Create MongoDB connection                                  │
   │                                                            │
   │ Schema Inference:                                          │
   │   • Sample N documents from collection                     │
   │   • Collect all field paths (nested traversal)             │
   │   • Resolve type conflicts (frequency analysis)            │
   │   • Build column names and types                           │
   │                                                            │
   │ Return schema to DuckDB                                    │
   └────────────────────────────────────────────────────────────┘
                              │
                              ▼
2. INIT PHASE (happens once per query)
   ┌────────────────────────────────────────────────────────────┐
   │ Get collection reference                                   │
   │ Convert DuckDB WHERE filters → MongoDB $match query        │
   │   • Parse table filters from query plan                    │
   │   • Convert to MongoDB operators ($eq, $gt, $gte, etc.)    │
   │   • Merge multiple filters on same column                  │
   │                                                            │
   │ Create MongoDB cursor:                                     │
   │   • Execute find() with $match filter                      │
   │   • MongoDB applies filters using indexes                  │
   │   • Returns cursor iterator                                │
   └────────────────────────────────────────────────────────────┘
                              │
                              ▼
3. EXECUTION PHASE (called repeatedly for each chunk)
   ┌────────────────────────────────────────────────────────────┐
   │ Fetch documents from cursor:                               │
   │   • Retrieve BSON documents from MongoDB                   │
   │                                                            │
   │ For each document:                                         │
   │   • Parse BSON structure                                   │
   │   • Extract fields by path                                 │
   │   • Convert BSON types → DuckDB types                      │ 
   │   • Flatten nested structures                              │
   │   • Write to columnar DataChunk                            │
   │                                                            │
   │ Return chunk to DuckDB (up to STANDARD_VECTOR_SIZE rows)   │
   └────────────────────────────────────────────────────────────┘
                              │
                              ▼
   ┌────────────────────────────────────────────────────────────┐
   │ DuckDB processes chunk:                                    │
   │   • Filters already applied in MongoDB (via pushdown)      │
   │   • Performs aggregations, joins, etc.                     │
   │   • Requests next chunk if needed                          │
   └────────────────────────────────────────────────────────────┘
```

### Pushdown Strategy

The extension uses a selective pushdown strategy that leverages MongoDB's indexing capabilities while utilizing DuckDB's analytical strengths:

**Pushed Down to MongoDB:** Filters (WHERE clauses), LIMIT clauses, and projections to reduce data transfer and leverage MongoDB indexes.

**Kept in DuckDB:** Aggregations, joins, and complex SQL features (window functions, CTEs, subqueries) where DuckDB's query optimizer and execution engine excel.

This hybrid approach provides fast indexed filtering from MongoDB combined with powerful analytical processing from DuckDB, optimizing performance for both simple filtered queries and complex analytical workloads.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

See LICENSE file for details.

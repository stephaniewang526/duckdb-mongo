# duckdb-mongo

Integrates DuckDB with MongoDB, enabling direct SQL queries over MongoDB collections without exporting data or ETL.

## Quick Start

```sql
-- Attach to MongoDB
ATTACH 'host=localhost port=27017' AS mongo_db (TYPE MONGO);

-- Query your collections
SELECT * FROM mongo_db.mydb.mycollection LIMIT 10;
```

**Using Secrets with MongoDB Atlas (recommended for production):**

See the [DuckDB Secrets Manager documentation](https://duckdb.org/docs/stable/configuration/secrets_manager) for more details on managing secrets.

```sql
-- Create a secret with MongoDB Atlas credentials
CREATE SECRET mongo_creds (
    TYPE mongo,
    HOST 'cluster0.xxxxx.mongodb.net',
    USER 'myuser',
    PASSWORD 'mypassword',
    SRV 'true'
);

-- Attach using the secret (use readPreference=secondaryPreferred for replica sets)
ATTACH 'dbname=mydb?readPreference=secondaryPreferred' AS atlas_db (TYPE mongo, SECRET 'mongo_creds');

-- Query your collections
SELECT * FROM atlas_db.mydb.mycollection;
```

## Features

- Direct SQL queries over MongoDB collections (no ETL/export)
- **MongoDB Atlas support** via connection strings or DuckDB Secrets
- Automatic schema inference (samples 100 documents by default)
- Nested document flattening with underscore-separated names
- BSON type mapping to DuckDB SQL types
- **Filter pushdown**: WHERE clauses pushed to MongoDB to leverage indexes
- Optional MongoDB query filters
- Read-only (write support may be added)

## Installation

### From Community Extensions (Recommended)

The easiest way to install the mongo extension is from the DuckDB community extensions repository:

```sql
INSTALL mongo FROM community;
LOAD mongo;
```

After installation, you can use the extension as described in the [Connecting to MongoDB](#connecting-to-mongodb) section.

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

**1. Key-value format:**
```sql
ATTACH 'host=localhost port=27017' AS mongo_db (TYPE MONGO);
ATTACH 'host=localhost port=27017 dbname=mydb' AS mongo_db (TYPE MONGO);
ATTACH 'host=cluster0.xxxxx.mongodb.net user=myuser password=mypass srv=true' AS atlas_db (TYPE MONGO);
```

**2. MongoDB URI format:**
```sql
ATTACH 'mongodb://user:pass@localhost:27017/mydb' AS mongo_db (TYPE MONGO);
```

**Connection Parameters:**

| Name | Description | Default | Applies To Format |
|------|-------------|---------|-------------------|
| `host` | MongoDB hostname or IP address | `localhost` | Both 1 and 2 |
| `port` | MongoDB port number | `27017` | Both 1 and 2 |
| `user` / `username` | MongoDB username | - | Both 1 and 2 |
| `password` | MongoDB password | - | Both 1 and 2 |
| `dbname` / `database` | Specific MongoDB database to connect to | - | Both 1 and 2 |
| `authsource` | Authentication database | - | Both 1 and 2 |
| `srv` | Use SRV connection format (for MongoDB Atlas) | `false` | Both 1 and 2 |
| `options` | Additional MongoDB connection string query parameters | - | Format 1 only |

> **Tip:** For replica sets (including MongoDB Atlas), use `readPreference=secondaryPreferred` to route reads to secondaries.

### Using DuckDB Secrets

Store credentials securely using [DuckDB Secrets](https://duckdb.org/docs/stable/configuration/secrets_manager) instead of embedding them in connection strings:

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
ATTACH 'dbname=mydb?readPreference=secondaryPreferred' AS atlas_db (TYPE mongo, SECRET 'mongo_creds');
```

**Default secret:** Create an unnamed secret to use as the default for all ATTACH operations:
```sql
CREATE SECRET (TYPE mongo, HOST 'localhost', USER 'myuser', PASSWORD 'mypass');
ATTACH '' AS mongo_db (TYPE mongo);  -- Uses __default_mongo automatically
ATTACH 'dbname=mydb' AS mongo_db (TYPE mongo);  -- Options merge with secret
```

> **Note:** An explicit database alias (`AS alias_name`) is required. The `dbname` parameter specifies which MongoDB database to connect to, not the DuckDB database name.

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

### Setting Up Test Data (For Examples)

To follow along with the examples in this README, you can create a test database with sample data:

**Option 1: Use the test script (recommended)**

```bash
bash test/create-mongo-tables.sh
```

**Option 2: Manual setup with mongosh**

```bash
mongosh "mongodb://localhost:27017/duckdb_mongo_test" --eval "db.orders.insertMany([{order_id: 'ORD-001', items: [{product: 'Laptop', quantity: 1, price: 999.99}, {product: 'Mouse', quantity: 2, price: 29.99}], total: 1059.97, status: 'completed'}, {order_id: 'ORD-002', items: [{product: 'Desk', quantity: 1, price: 299.99}], total: 299.99, status: 'pending'}, {order_id: 'ORD-003', items: [], total: 0, status: 'cancelled'}, {order_id: 'ORD-004', items: [{product: 'Keyboard', quantity: 1}], total: 79.99, status: 'pending'}]);"
```

**Option 3: Interactive mongosh**

```bash
mongosh "mongodb://localhost:27017/duckdb_mongo_test"
```

Then paste:
```javascript
db.orders.insertMany([
  { order_id: 'ORD-001', items: [{ product: 'Laptop', quantity: 1, price: 999.99 }, { product: 'Mouse', quantity: 2, price: 29.99 }], total: 1059.97, status: 'completed' },
  { order_id: 'ORD-002', items: [{ product: 'Desk', quantity: 1, price: 299.99 }], total: 299.99, status: 'pending' },
  { order_id: 'ORD-003', items: [], total: 0, status: 'cancelled' },
  { order_id: 'ORD-004', items: [{ product: 'Keyboard', quantity: 1 }], total: 79.99, status: 'pending' }
]);
```

After setting up test data, attach to the test database:

```sql
ATTACH 'host=localhost port=27017 dbname=duckdb_mongo_test' AS mongo_test (TYPE MONGO);
```

### Basic Queries

```sql
-- Attach to MongoDB (using test database from setup above)
ATTACH 'host=localhost port=27017 dbname=duckdb_mongo_test' AS mongo_test (TYPE MONGO);

-- Show attached databases
SHOW DATABASES;
┌───────────────┐
│ database_name │
│    varchar    │
├───────────────┤
│ memory        │
│ mongo_test    │
└───────────────┘

-- List schemas (MongoDB databases) in the attached catalog
SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'mongo_test' ORDER BY schema_name;
┌───────────────────┐
│    schema_name    │
│      varchar      │
├───────────────────┤
│ duckdb_mongo_test │
│ main              │
└───────────────────┘

-- Select data from a specific collection
SELECT order_id, status, total FROM mongo_test.duckdb_mongo_test.orders;
┌──────────┬───────────┬─────────┐
│ order_id │  status   │  total  │
│ varchar  │  varchar  │ double  │
├──────────┼───────────┼─────────┤
│ ORD-001  │ completed │ 1059.97 │
│ ORD-002  │ pending   │  299.99 │
│ ORD-003  │ cancelled │     0.0 │
│ ORD-004  │ pending   │   79.99 │
└──────────┴───────────┴─────────┘

-- Query arrays of objects using list_extract (1-based indexing)
SELECT order_id, list_extract(items, 1).product AS product, list_extract(items, 1).price AS price FROM mongo_test.duckdb_mongo_test.orders;
┌──────────┬──────────┬────────┐
│ order_id │ product  │ price  │
│ varchar  │ varchar  │ double │
├──────────┼──────────┼────────┤
│ ORD-001  │ Laptop   │ 999.99 │
│ ORD-002  │ Desk     │ 299.99 │
│ ORD-003  │ NULL     │   NULL │
│ ORD-004  │ Keyboard │   NULL │
└──────────┴──────────┴────────┘

-- Expand arrays into multiple rows using UNNEST
SELECT order_id, UNNEST(items).product AS product, UNNEST(items).price AS price 
FROM mongo_test.duckdb_mongo_test.orders 
WHERE order_id = 'ORD-001';
┌──────────┬──────────┬─────────┐
│ order_id │ product  │  price  │
│ varchar  │ varchar  │ double  │
├──────────┼──────────┼─────────┤
│ ORD-001  │ Laptop   │  999.99 │
│ ORD-001  │ Mouse    │   29.99 │
└──────────┴──────────┴─────────┘

-- Query with aggregation
SELECT status, COUNT(*) as count, SUM(total) as total_revenue 
  FROM mongo_test.duckdb_mongo_test.orders 
  GROUP BY status
  ORDER BY status;
┌───────────┬───────┬───────────────┐
│  status   │ count │ total_revenue │
│  varchar  │ int64 │    double     │
├───────────┼───────┼───────────────┤
│ cancelled │     1 │           0.0 │
│ completed │     1 │       1059.97 │
│ pending   │     2 │        379.98 │
└───────────┴───────┴───────────────┘

-- Filter on array element fields using UNNEST
SELECT DISTINCT order_id FROM mongo_test.duckdb_mongo_test.orders, UNNEST(items) AS unnest 
WHERE unnest.product = 'Mouse';
┌──────────┐
│ order_id │
│ varchar  │
├──────────┤
│ ORD-001  │
└──────────┘
```

### Using mongo_scan Directly

You can also use the `mongo_scan` table function directly without attaching:

```sql
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection');
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection', 
                         filter := '{"status": "active"}', sample_size := 200);
```

### Cache Management

When using `ATTACH` to connect to MongoDB, the extension caches schema information, collection names, and view metadata to improve query performance. If the MongoDB schema changes (e.g., new collections are added, or collection schemas change), you may need to clear the cache:

```sql
-- Clear all MongoDB caches for all attached databases
SELECT * FROM mongo_clear_cache();
```

This function clears all caches for all attached MongoDB databases:
- Collection names cache
- View info cache (including schema information)
- Schema cache

> **Note:** Currently, cache clearing is all-or-nothing (all databases). Selective cache clearing for specific databases or collections is not yet supported.

After clearing the cache, the next query will re-scan schemas and re-infer collection schemas.

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
| `Array` | `LIST` or `VARCHAR` | `LIST(STRUCT(...))` for arrays of objects, `LIST(primitive)` for arrays of primitives, `LIST(LIST(...))` for arrays of arrays (including `LIST(LIST(STRUCT(...)))` for arrays of arrays of objects) (see [Array Handling](#array-handling) below) |
| `Document` | `VARCHAR` | Stored as JSON string |
| Other | `VARCHAR` | Default for unknown types |

### Schema Inference

The extension automatically infers schemas by sampling documents (default: 100, configurable via `sample_size`):

- **Nested Documents**: Flattened with underscore-separated names (e.g., `user_address_city`), up to 5 levels deep
- **Type Conflicts**: Frequency-based resolution:
  - VARCHAR if >70% of values are strings
  - DOUBLE if ≥30% are doubles (or any doubles present)
  - BIGINT if ≥30% are integers (when no doubles)
  - BOOLEAN/TIMESTAMP if ≥70% match
  - Defaults to VARCHAR
- **Missing Fields**: NULL values

#### Array Handling

**Arrays of Objects:**
- Arrays of objects are stored as DuckDB `LIST(STRUCT(...))` types
- **Schema Inference**: Scans up to **10 elements** per array to discover all field names across array elements
  - This ensures fields that only exist in later elements are still discovered
  - Example: If `items[0]` has `{product, quantity}` and `items[5]` has `{product, quantity, discount}`, the `discount` field will be included in the STRUCT
  - Creates a LIST type containing a STRUCT with all discovered fields
  
- **Querying Arrays:** Use `list_extract()` to access specific elements (1-based indexing) or `UNNEST()` to expand arrays into multiple rows. See [Basic Queries](#basic-queries) for examples.

**Arrays of Primitives:**
- Arrays of primitives (strings, numbers) are stored as `LIST` types
- Example: `tags: ['admin', 'user']` → `LIST(VARCHAR)` containing `['admin', 'user']`
- Can be queried with list_extract (1-based indexing): `list_extract(tags, 1)` returns `'admin'`
- Can be expanded with UNNEST: `SELECT UNNEST(tags) FROM mongo_test.duckdb_mongo_test.users`

**Arrays of Arrays:**
- Arrays of arrays are stored as `LIST(LIST(...))` types
- Supports nested arrays of any depth (up to 5 levels)
- Example: `matrix: [[1,2], [3,4]]` → `LIST(LIST(BIGINT))` containing `[[1,2],[3,4]]`
- Example: `data: [[[1,2], [3,4]], [[5,6], [7,8]]]` → `LIST(LIST(LIST(BIGINT)))` for 3D arrays
- Arrays of arrays of objects: `data: [[{x: 1}, {x: 2}], [{x: 3}, {x: 4}]]` → `LIST(LIST(STRUCT(...)))`
- Can be queried with nested list_extract (1-based indexing):
  - For 2D arrays: `list_extract(list_extract(matrix, 1), 2)` returns `2` (second element of first row)
  - For 3D arrays: `list_extract(list_extract(list_extract(data, 1), 1), 2)` returns `2` (second element of first row of first layer)

**Mixed Array Depths:**
- When documents in a collection have arrays of different depths, the schema inference uses the **deepest depth** found across all sampled documents
- Documents with shallower arrays are automatically **wrapped** to match the expected depth, allowing all arrays to be returned as DuckDB LIST types
- Example: If one document has `data: [[[1,2], [3,4]]]` (3D) and another has `data: [[1,2], [3,4]]` (2D), the schema infers `LIST(LIST(LIST(BIGINT)))` (3D)
  - The 2D array `[[1,2], [3,4]]` is automatically wrapped to `[[[1,2]], [[3,4]]]` to match the 3D schema
  - Both documents return valid LIST values that can be queried using DuckDB's LIST functions
- This ensures data is preserved and queryable even when array structures vary across documents

### Limitations

- Read-only
- Schema inferred from sample (may miss fields)
- Schema re-inferred per query when using `mongo_scan` table function directly (schema is cached when using `ATTACH` catalog views; use `mongo_clear_cache()` to invalidate cache)
- **Nested documents in arrays**: Nested documents within array elements are stored as VARCHAR (JSON strings) rather than nested STRUCT types
  - Example: `items: [{product: 'Laptop', specs: {cpu: 'Intel', ram: '16GB'}}]` → `specs` field is VARCHAR, not STRUCT
  - This is a simplification to avoid complex nested STRUCT handling

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

The extension uses a selective pushdown strategy: **filter at MongoDB** (reduce data transfer), **analyze in DuckDB** (powerful SQL).

**Pushed Down to MongoDB:**
- WHERE clauses (automatic conversion to MongoDB `$match` queries)
- LIMIT clauses (when directly above table scan)
- Manual `filter` parameter (for MongoDB-specific operators like `$elemMatch`)

**Kept in DuckDB:**
- Aggregations, joins, window functions, CTEs, subqueries, ORDER BY

#### Automatic Filter Pushdown

WHERE clauses are automatically converted to MongoDB `$match` queries. Use `EXPLAIN` to see which operations are pushed down:

```sql
-- Filter pushed down to MongoDB
EXPLAIN SELECT * FROM mongo_test.duckdb_mongo_test.orders WHERE status = 'completed';
```

The plan shows filters in `MONGO_SCAN`, indicating pushdown:

```
┌─────────────────────────────┐
│┌───────────────────────────┐│
││       Physical Plan       ││
│└───────────────────────────┘│
└─────────────────────────────┘
┌───────────────────────────┐
│        MONGO_SCAN         │
│    ────────────────────   │
│    Function: MONGO_SCAN   │
│                           │
│          Filters:         │
│     status='completed'    │  ← Pushed to MongoDB
│                           │
│           ~1 row          │
└───────────────────────────┘
```

For aggregations, filters are pushed down while aggregation happens in DuckDB:

**Supported Filter Operations:**

- **Comparison operators**: `=`, `!=`, `<`, `<=`, `>`, `>=`
- **IN clauses**: `WHERE status IN ('active', 'pending')` → MongoDB `{status: {$in: ['active', 'pending']}}`
- **NULL checks**: `IS NULL` and `IS NOT NULL`
- **Multiple conditions**: AND/OR combinations merged into efficient MongoDB queries
- **Nested fields**: Flattened fields (e.g., `address_city`) converted to dot notation (`address.city`)

**Examples:**

```sql
-- Equality (using test data)
SELECT * FROM mongo_test.duckdb_mongo_test.orders WHERE status = 'completed';
-- MongoDB: {status: 'completed'}

-- Range (example with users collection)
SELECT * FROM mongo_test.duckdb_mongo_test.users WHERE age > 28 AND age < 40;
-- MongoDB: {age: {$gt: 28, $lt: 40}}

-- IN
SELECT * FROM mongo_test.duckdb_mongo_test.orders WHERE status IN ('completed', 'pending');
-- MongoDB: {status: {$in: ['completed', 'pending']}}

-- Nested field (example with users collection)
SELECT * FROM mongo_test.duckdb_mongo_test.users WHERE address_city = 'New York';
-- MongoDB: {'address.city': 'New York'}
```

> **Note:** When using `mongo_scan` directly, you can provide an optional `filter` parameter (e.g., `filter := '{"status": "active"}'`) for MongoDB-specific operators. If both WHERE clauses and `filter` are present, WHERE clauses take precedence.

> **Note:** Filters on array elements (using `UNNEST`) are **not** pushed down to MongoDB—they are applied in DuckDB after expanding arrays. This means **all documents** are fetched from MongoDB, then filtered in DuckDB. For large collections, consider using MongoDB's `$elemMatch` operator via the `filter` parameter in `mongo_scan` to filter at the database level. See [Basic Queries](#basic-queries) for array filtering examples.

#### LIMIT Pushdown

LIMIT is automatically pushed down when directly above the table scan (without ORDER BY, aggregations, or joins):

```sql
SELECT * FROM mongo_test.duckdb_mongo_test.orders LIMIT 10;
-- MongoDB uses: .limit(10)
```

**Limitation:** When ORDER BY is present, DuckDB uses `TOP_N` which handles ordering in DuckDB, so LIMIT is not pushed down.

#### Projection Pushdown

Not yet implemented. All columns are currently fetched from MongoDB.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

See LICENSE file for details.

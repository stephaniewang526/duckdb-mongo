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

**Tip:** For replica sets (including MongoDB Atlas), use `readPreference=secondaryPreferred` to route reads to secondaries.

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
└───────────────────┘

-- Select data from a specific collection
SELECT order_id, status, total FROM mongo_db.duckdb_mongo_test.orders;
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
SELECT order_id, list_extract(items, 1).product AS product, list_extract(items, 1).price AS price FROM mongo_db.duckdb_mongo_test.orders;
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
FROM mongo_db.duckdb_mongo_test.orders 
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
  FROM mongo_db.duckdb_mongo_test.orders 
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
SELECT DISTINCT order_id FROM mongo_db.duckdb_mongo_test.orders, UNNEST(items) AS unnest 
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
- **Type Conflicts**: Frequency-based resolution (VARCHAR if >70%, numeric if ≥30%, defaults to VARCHAR)
- **Missing Fields**: NULL values

#### Array Handling

**Arrays of Objects:**
- Arrays of objects are stored as DuckDB `LIST(STRUCT(...))` types
- **Schema Inference**: Scans up to **10 elements** per array to discover all field names across array elements
  - This ensures fields that only exist in later elements are still discovered
  - Example: If `items[0]` has `{product, quantity}` and `items[5]` has `{product, quantity, discount}`, the `discount` field will be included in the STRUCT
  - Creates a LIST type containing a STRUCT with all discovered fields
  
- **Querying Arrays:**
  ```sql
  -- Return full array
  SELECT order_id, items FROM orders WHERE order_id = 'ORD-001';
  -- Returns: [{'product': 'Laptop', 'quantity': 1, 'price': 999.99}, {'product': 'Mouse', 'quantity': 2, 'price': 29.99}]
  
  -- Access specific array element using list_extract (1-based indexing)
  SELECT order_id, list_extract(items, 1).product AS product, list_extract(items, 1).price AS price FROM orders;
  -- Returns: 'Laptop', 999.99 (from first element)
  
  SELECT order_id, list_extract(items, 2).product AS product, list_extract(items, 2).price AS price FROM orders;
  -- Returns: 'Mouse', 29.99 (from second element)
  
  -- Expand array into multiple rows using UNNEST
  SELECT order_id, UNNEST(items).product AS product, UNNEST(items).price AS price FROM orders;
  -- Returns multiple rows, one per array element
  ```
  
  **Note:** For filtering on array elements, see the [Pushdown Strategy](#pushdown-strategy) section.

**Arrays of Primitives:**
- Arrays of primitives (strings, numbers) are stored as `LIST` types
- Example: `tags: ['admin', 'user']` → `LIST(VARCHAR)` containing `['admin', 'user']`
- Can be queried with list_extract (1-based indexing): `list_extract(tags, 1)` returns `'admin'`
- Can be expanded with UNNEST: `SELECT UNNEST(tags) FROM users`

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
- Schema re-inferred per query
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

The extension uses a selective pushdown strategy that leverages MongoDB's indexing capabilities while utilizing DuckDB's analytical strengths:

**Pushed Down to MongoDB:** Filters (WHERE clauses) and LIMIT clauses to reduce data transfer and leverage MongoDB indexes.

**Kept in DuckDB:** Aggregations, joins, and complex SQL features (window functions, CTEs, subqueries) where DuckDB's query optimizer and execution engine excel.

This hybrid approach provides fast indexed filtering from MongoDB combined with powerful analytical processing from DuckDB, optimizing performance for both simple filtered queries and complex analytical workloads.

#### Filter Pushdown

WHERE clauses are automatically converted to MongoDB `$match` queries, allowing MongoDB to leverage indexes for efficient filtering.

**Supported Filter Operations:**

- **Comparison operators**: `=`, `!=`, `<`, `<=`, `>`, `>=`
- **IN clauses**: `WHERE status IN ('active', 'pending')` → `{status: {$in: ['active', 'pending']}}`
- **NULL checks**: `IS NULL` and `IS NOT NULL`
- **Multiple conditions**: AND/OR combinations are merged into efficient MongoDB queries
- **Nested fields**: Filters on flattened nested fields (e.g., `address_city`) are converted to dot notation

**Array Filtering:**

DuckDB doesn't support direct field access on LIST types (e.g., `items.product`). To filter on array elements, use `UNNEST` to expand the array first:

```sql
-- Filter on array element fields using UNNEST
SELECT DISTINCT order_id FROM orders, UNNEST(items) AS unnest WHERE unnest.product = 'Mouse';

-- Multiple conditions on same array element
SELECT DISTINCT order_id FROM orders, UNNEST(items) AS unnest 
WHERE unnest.product = 'Laptop' AND unnest.quantity = 1;

-- Comparison operators on arrays
SELECT DISTINCT order_id FROM orders, UNNEST(items) AS unnest WHERE unnest.quantity > 2;
```

**Note:** The `DISTINCT` keyword is used to avoid duplicate `order_id` values when multiple array elements match the filter condition.

**Filter Examples:**

```sql
-- Simple equality filter
SELECT * FROM users WHERE active = true;
-- MongoDB query: {active: true}

-- Range filter
SELECT * FROM users WHERE age > 28 AND age < 40;
-- MongoDB query: {age: {$gt: 28, $lt: 40}}

-- IN filter
SELECT * FROM users WHERE status IN ('active', 'pending');
-- MongoDB query: {status: {$in: ['active', 'pending']}}

-- NULL check
SELECT * FROM users WHERE email IS NOT NULL;
-- MongoDB query: {email: {$ne: null}}

-- Nested field filter
SELECT * FROM users WHERE address_city = 'New York';
-- MongoDB query: {'address.city': 'New York'}
```

#### LIMIT Pushdown

Constant LIMIT values are pushed down to MongoDB's `limit()` option, reducing data transfer for TOP N queries:

```sql
SELECT * FROM orders ORDER BY total DESC LIMIT 10;
-- MongoDB query uses: .limit(10)
```

**Limitation:** LIMIT pushdown only works when LIMIT is directly above the table scan. For queries with joins or aggregations before LIMIT, the limit is applied in DuckDB after processing.

#### Projection Pushdown

Projection pushdown (fetching only selected columns) is not yet implemented. All columns are currently fetched from MongoDB, though this is planned for future optimization.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

See LICENSE file for details.

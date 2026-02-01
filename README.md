# duckdb-mongo

Integrates DuckDB with MongoDB, enabling direct SQL queries over MongoDB collections without exporting data or ETL.

## Announcement

We currently support DuckDB `v1.4.4`. The extension is built against the DuckDB submodule in this repo, and that
submodule is updated when new DuckDB releases are validated. See
[How to Maintain an Extension Through DuckDB Releases](https://duckdb.org/community_extensions/development#how-to-maintain-an-extension-through-duckdb-releases)
for the community extension policy.

> **Note:** Community extensions are built and distributed only for the latest stable DuckDB release. Older DuckDB
> versions can keep using the last compatible mongo extension build, but they will not receive updates. To keep
> getting new mongo extension releases, upgrade DuckDB to the latest stable version. To confirm the version
> locally, run `SELECT version();` in DuckDB (or `duckdb --version` if you are using the CLI).

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
- **TLS/SSL encryption** for secure connections
- **Flexible schema handling**: User-provided schemas, `__schema` document support (Atlas SQL compatibility), or automatic inference
- Nested document flattening with underscore-separated names
- **BSON type mapping** including Decimal128, arrays, and nested documents (see [BSON Type Mapping](#bson-type-mapping))
- **Query pushdown** to reduce data transfer (see [Pushdown Strategy](#pushdown-strategy)):
  - Filters (WHERE clauses, complex expressions, semi-join IN)
  - Projections (SELECT columns)
  - Limits and TopN (ORDER BY _id LIMIT N)
  - Aggregations (COUNT, SUM, MIN, MAX, AVG with GROUP BY)
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
| `tls` / `ssl` | Enable TLS/SSL encryption | `false` | Both 1 and 2 |
| `tls_ca_file` | Path to CA certificate file | - | Both 1 and 2 |
| `tls_allow_invalid_certificates` | Allow invalid certificates (for testing only) | `false` | Both 1 and 2 |
| `options` | Additional MongoDB connection string query parameters | - | Format 1 only |

> **Tip:** For replica sets (including MongoDB Atlas), use `readPreference=secondaryPreferred` to route reads to secondaries.

### TLS/SSL Connections

The extension supports TLS/SSL encrypted connections for secure MongoDB access. Enable TLS by setting `tls=true` or `ssl=true`:

```sql
-- Basic TLS connection
ATTACH 'host=mongodb.example.com port=27017 user=myuser password=mypass tls=true' AS mongo_secure (TYPE MONGO);

-- TLS with custom CA certificate file
ATTACH 'host=mongodb.example.com port=27017 user=myuser password=mypass tls=true tls_ca_file=/path/to/ca.pem' AS mongo_secure (TYPE MONGO);
```

**Using Secrets with TLS:**

```sql
CREATE SECRET mongo_tls_secret (
    TYPE mongo,
    HOST 'mongodb.example.com',
    PORT '27017',
    USER 'myuser',
    PASSWORD 'mypass',
    TLS 'true',
    TLS_CA_FILE '/path/to/ca.pem'
);

ATTACH 'dbname=mydb' AS mongo_secure (TYPE mongo, SECRET 'mongo_tls_secret');
```

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

- **Without `dbname`**: Creates a schema for each MongoDB database plus a `main` schema; defaults to `"main"`
- **With `dbname`**: Creates only the specified database schema; defaults to that schema

```sql
ATTACH 'host=localhost port=27017' AS mongo_all (TYPE MONGO);
USE mongo_all;  -- Defaults to "main", but all databases available as schemas

ATTACH 'host=localhost port=27017 dbname=mydb' AS mongo_db (TYPE MONGO);
USE mongo_db;  -- Defaults to "mydb" (only schema available)
```

## Querying MongoDB

### Setting Up Test Data (For Examples)

**Prerequisites:**
- MongoDB instance running (e.g., `docker run -d -p 27017:27017 mongo` or local MongoDB installation)
- `mongosh` installed

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

-- List schemas in the attached catalog (only the specified database when using dbname=)
SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'mongo_test';
┌───────────────────┐
│    schema_name    │
│      varchar      │
├───────────────────┤
│ duckdb_mongo_test │
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
-- Basic usage
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection');

-- With filter and sample size
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection', 
                         filter := '{"status": "active"}', sample_size := 200);

-- With explicit schema
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection',
                         columns := {'_id': 'VARCHAR', 'name': 'VARCHAR', 'age': 'BIGINT'});

-- With nested path mapping
SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection',
                         columns := {
                             '_id': 'VARCHAR',
                             'name': 'VARCHAR',
                             'city': {'type': 'VARCHAR', 'path': 'address.city'}
                         });
```

**Parameters:**
- `connection_string`: MongoDB connection string
- `database`: MongoDB database name
- `collection`: MongoDB collection name
- `filter` (optional): MongoDB query filter as JSON string (e.g., `'{"status": "active"}'`)
- `sample_size` (optional): Number of documents to sample for schema inference (default: 100)
- `columns` (optional): Explicit schema definition as a struct (see [Schema Resolution](#schema-resolution) for details)
- `schema_mode` (optional): How to handle type mismatches: `'permissive'` (default), `'dropmalformed'`, or `'failfast'` (see [Schema Enforcement Modes](#schema-enforcement-modes))

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
| `Decimal128` | `DOUBLE` | High-precision decimals converted to double (may lose precision) |
| `Boolean` | `BOOLEAN` | |
| `Date` | `TIMESTAMP` / `DATE` | `DATE` if time component is midnight UTC, else `TIMESTAMP` |
| `ObjectId` | `VARCHAR` | 24-character hex string |
| `Binary` | `BLOB` | |
| `Array` | `LIST` or `VARCHAR` | `LIST(STRUCT(...))` for arrays of objects, `LIST(primitive)` for arrays of primitives, `LIST(LIST(...))` for arrays of arrays (see [Array Handling](#array-handling)) |
| `Document` | `VARCHAR` | Nested documents stored as JSON string |
| `Null`, `Undefined` | `VARCHAR` | Type refined from other documents during inference |
| `Regex`, `Code`, `Symbol`, `Timestamp`, `MinKey`, `MaxKey` | `VARCHAR` | Special BSON types stored as string representation |

### Schema Resolution

The extension uses a three-tier schema resolution strategy with the following priority order:

1. **User-provided `columns` parameter** (highest priority)
2. **`__schema` document in collection** (for Atlas SQL compatibility)
3. **Automatic schema inference** (fallback)

#### User-Provided Schema

You can explicitly specify the schema using the `columns` parameter when calling `mongo_scan`:

**Simple Format:**
```sql
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017',
    'mydb',
    'mycollection',
    columns := {'_id': 'VARCHAR', 'name': 'VARCHAR', 'age': 'BIGINT', 'active': 'BOOLEAN'}
);
```

**Nested Format with Path Mapping:**
For nested fields, you can map column names to MongoDB dot notation paths:
```sql
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017',
    'mydb',
    'mycollection',
    columns := {
        '_id': 'VARCHAR',
        'name': 'VARCHAR',
        'city': {'type': 'VARCHAR', 'path': 'address.city'},
        'street': {'type': 'VARCHAR', 'path': 'address.street'}
    }
);
```

The `columns` parameter accepts:
- **Simple format**: `'column_name': 'TYPE'` where TYPE is a DuckDB type string (e.g., `'VARCHAR'`, `'BIGINT'`, `'DOUBLE'`, `'BOOLEAN'`, `'DATE'`, `'TIMESTAMP'`)
- **Nested format**: `'column_name': {'type': 'TYPE', 'path': 'mongo.path'}` for mapping to nested MongoDB fields

#### __schema Document (Atlas SQL Compatibility)

For MongoDB Atlas SQL compatibility, you can store a schema document in your collection with `_id: "__schema"`. The extension will automatically detect and use this schema.

**Simple Format** (schema fields directly in document):
```javascript
{
  "_id": "__schema",
  "name": "VARCHAR",
  "age": "BIGINT",
  "email": "VARCHAR"
}
```

**Nested Format** (schema in nested `schema` field):
```javascript
{
  "_id": "__schema",
  "schema": {
    "name": "VARCHAR",
    "age": "BIGINT",
    "email": "VARCHAR"
  }
}
```

**Path Mapping Format** (for nested MongoDB fields):
```javascript
{
  "_id": "__schema",
  "name": "VARCHAR",
  "city": {"type": "VARCHAR", "path": "address.city"},
  "street": {"type": "VARCHAR", "path": "address.street"}
}
```

> **Note:** When using `ATTACH` to connect to MongoDB, the `__schema` document is cached along with other schema information. Use `mongo_clear_cache()` to invalidate the cache after schema changes.

#### Schema Inference

When neither user-provided schema nor `__schema` document is available, the extension automatically infers schemas by sampling documents (default: 100, configurable via `sample_size`):

- **Nested Documents**: Flattened with underscore-separated names (e.g., `user_address_city`), up to 5 levels deep
- **Type Conflicts**: Frequency-based resolution:
  - VARCHAR if >70% of values are strings
  - DOUBLE if ≥30% are doubles (or any doubles present)
  - BIGINT if ≥30% are integers (when no doubles)
  - BOOLEAN/TIMESTAMP if ≥70% match
  - Defaults to VARCHAR
- **Missing Fields**: NULL values

#### Schema Enforcement Modes

When using an explicit schema (via `columns` parameter or `__schema` document), you can control how the extension handles documents that don't match the expected types using the `schema_mode` parameter:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `permissive` | Set invalid fields to NULL (default) | Exploratory analysis, fault-tolerant pipelines |
| `dropmalformed` | Skip entire rows with schema violations | Data quality filtering, clean datasets |
| `failfast` | Throw error immediately on first mismatch | Production pipelines, data contracts |

**Examples:**

```sql
-- PERMISSIVE (default): Invalid values become NULL
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017', 'mydb', 'mycol',
    columns := {'name': 'VARCHAR', 'age': 'INTEGER'},
    schema_mode := 'permissive'
);

-- DROPMALFORMED: Skip rows where 'age' is not a valid integer
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017', 'mydb', 'mycol',
    columns := {'name': 'VARCHAR', 'age': 'INTEGER'},
    schema_mode := 'dropmalformed'
);

-- FAILFAST: Throw error if any document has invalid 'age' value
SELECT * FROM mongo_scan(
    'mongodb://localhost:27017', 'mydb', 'mycol',
    columns := {'name': 'VARCHAR', 'age': 'INTEGER'},
    schema_mode := 'failfast'
);
```

> **Note:** Schema enforcement only applies when an explicit schema is provided (via `columns` or `__schema`). Inferred schemas use permissive behavior regardless of the `schema_mode` setting.
>
> When using `dropmalformed` or `failfast`, certain query optimizations are disabled to ensure accurate validation (e.g., aggregate pushdowns run in DuckDB instead of MongoDB, and all schema columns are fetched for validation). For best performance with large collections, use `permissive` (the default) unless strict enforcement is required.

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
- Schema inference (when used as fallback) samples documents and may miss fields that don't appear in the sample
- Schema re-inferred per query when using `mongo_scan` directly (cached when using `ATTACH`; use `mongo_clear_cache()` to invalidate)
- **Decimal128 precision**: Converted to DOUBLE, which may lose precision for high-precision decimal values
- **Nested documents in arrays**: Stored as VARCHAR (JSON strings) rather than nested STRUCT types
  - Example: `items: [{product: 'Laptop', specs: {cpu: 'Intel', ram: '16GB'}}]` → `specs` field is VARCHAR, not STRUCT

## Advanced Topics

### Architecture

The extension enables **in-process analytical SQL queries** over MongoDB data using DuckDB's embedded analytical engine. Queries execute against live MongoDB data in real-time, with analytical operations (joins, aggregations, window functions) performed locally in memory.

```
┌─────────────────────────────────────────┐
│           User/Application              │
└────────┬───────────────────────┬────────┘
         │                       ▲
         │ SQL Query             │ Result set (columnar)
         ▼                       │
┌─────────────────────────────────────────┐
│         DUCKDB ENGINE                   │
│  ┌───────────────────────────────────┐  │
│  │ Query Planning & Optimization     │  │
│  │ - Pushdown & Plan Optimization    │  │
│  └───────────────────────────────────┘  │
│  ┌───────────────────────────────────┐  │
│  │ Query Execution                   │  │
│  │ - Joins, Aggregations             │  │
│  │ - Window Functions, CTEs          │  │
│  └───────────────────────────────────┘  │
└────────┬───────────────────────┬────────┘
         │                       ▲
         │ mongo_scan            │ DataChunks
         ▼                       │
┌────────┴───────────────────────┴────────┐
│ duckdb-mongo Extension                  │
│  • Schema Resolution                    │
│  • Pushdown Optimization                │
│  • BSON → Columnar Conversion           │
└────────┬───────────────────────┬────────┘
         │                       ▲
         │ MQL                   │ BSON stream
         ▼                       │
┌─────────────────────────────────────────┐
│         MONGODB DATABASE                │
│  ┌───────────────────────────────────┐  │
│  │ Document Store Operations         │  │
│  │ - Query Execution ($match, $group)│  │
│  │ - Document Streaming (cursor)     │  │
│  └───────────────────────────────────┘  │
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
   │ Schema Resolution:                                         │
   │   • User-provided, __schema document, or inference         │
   │   • Build column names and types                           │
   │                                                            │
   │ Return schema to DuckDB                                    │
   └────────────────────────────────────────────────────────────┘
                              │
                              ▼
2. INIT PHASE (happens once per query)
   ┌────────────────────────────────────────────────────────────┐
   │ Build MongoDB query:                                       │
   │   • Filter pushdown ($match)                               │
   │   • Projection pushdown                                    │
   │   • Aggregation/Limit pushdown ($group, $limit)            │
   │                                                            │
   │ Create MongoDB cursor or aggregation pipeline              │
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

The extension uses a selective pushdown strategy: **filter at MongoDB** (reduce data transfer), **analyze in DuckDB** (analytical operations).

**Pushed Down to MongoDB:**
- WHERE clauses (automatic conversion to MongoDB `$match` queries)
- Column projections (only columns used in SELECT are fetched)
- LIMIT clauses: simple `LIMIT N` (cursor limit) and `ORDER BY _id LIMIT N` (aggregation pipeline)
- Manual `filter` parameter (for MongoDB-specific operators like `$elemMatch`)
- Aggregations: `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG` with optional `GROUP BY` (see [Aggregation Pushdown](#aggregation-pushdown))

**Kept in DuckDB:**
- Joins, window functions, CTEs, subqueries
- ORDER BY (except `ORDER BY _id LIMIT N` which is pushed down)

#### Automatic Filter Pushdown

WHERE clauses are automatically converted to MongoDB `$match` queries. Use `EXPLAIN` to see which operations are pushed down:

```sql
-- Filter pushed down to MongoDB
EXPLAIN SELECT order_id, status FROM mongo_test.duckdb_mongo_test.orders WHERE status = 'completed';
```

The plan shows filters and projections in `MONGO_SCAN`, indicating pushdown:

```
┌─────────────────────────────┐
│┌───────────────────────────┐│
││       Physical Plan       ││
│└───────────────────────────┘│
└─────────────────────────────┘
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│          order_id         │
│           status          │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│        MONGO_SCAN         │
│    ────────────────────   │
│    Function: MONGO_SCAN   │
│                           │
│        Projections:       │
│           status          │
│          order_id         │  ← Only SELECT columns fetched
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
- **Complex filters**: Function calls (e.g., `LENGTH(name) > 5`) and column-to-column comparisons (e.g., `age > balance`) pushed down as MongoDB `$expr` queries (see [Complex Filter Pushdown](#complex-filter-pushdown))

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

LIMIT is automatically pushed down when directly above the table scan:

```sql
SELECT * FROM mongo_test.duckdb_mongo_test.orders LIMIT 10;
-- MongoDB uses: .limit(10)
```

> **Note:** When `ORDER BY _id` is present with LIMIT, the extension uses TopN pushdown via aggregation pipeline (see [TopN Pushdown](#topn-pushdown)). For other ORDER BY columns, sorting is performed in DuckDB after fetching data.

#### Projection Pushdown

Projection pushdown automatically fetches only the columns used in the SELECT clause, reducing data transfer and serialization overhead.

**How it works:**
- DuckDB analyzes the query to identify which columns are needed
- Only those columns are included in the MongoDB projection
- Reduces network transfer and BSON parsing overhead

**Example:**
```sql
-- Only fetches order_id, status, and total (not items or other columns)
SELECT order_id, status, total FROM mongo_test.duckdb_mongo_test.orders WHERE status = 'completed';
-- MongoDB projection: {order_id: 1, status: 1, total: 1, _id: 1}
```

Use `EXPLAIN` to see which columns are projected:
```sql
EXPLAIN SELECT order_id, status FROM mongo_test.duckdb_mongo_test.orders;
```

The plan shows `Projections: order_id, status` in the `MONGO_SCAN` operator.

#### Filter Prune Optimization

Filter prune works together with projection pushdown to further reduce data transfer by excluding filter columns that are not used in the SELECT clause when filters are pushed down to MongoDB.

**How it works:**
- Filters are pushed down to MongoDB (server-side filtering)
- Filter columns not in the SELECT clause are excluded from the projection
- Only columns needed for query results are fetched

**Example:**
```sql
-- Filters on status and total, but only selects order_id
SELECT order_id 
FROM mongo_test.duckdb_mongo_test.orders 
WHERE status = 'completed' AND total > 500;
-- MongoDB: Filters pushed down, but only order_id is fetched
-- status and total are NOT fetched (filtered server-side)
```

Use `EXPLAIN` to verify filter prune is working. The plan shows filters that are pushed down but not included in projections:
```sql
EXPLAIN SELECT order_id FROM mongo_test.duckdb_mongo_test.orders WHERE status = 'completed' AND total > 500;
```

The plan shows `Projections: order_id` and `Filters: status='completed' AND total>500.0` in the `MONGO_SCAN` operator, indicating filters are pushed down but filter columns are not fetched.

#### Complex Filter Pushdown

Complex filter pushdown enables pushing complex filter expressions (function calls, column-to-column comparisons, etc.) to MongoDB using `$expr` queries. This allows MongoDB to filter server-side for expressions that cannot be handled by simple TableFilter pushdown.

**Supported Complex Filter Types:**

- **Function calls**: `LENGTH(name) > 5`, `CHAR_LENGTH(name) > 5`
- **Substring filters**: `SUBSTRING(name, 1, 3) = 'Ann'` (constant start/length only)
- **Column-to-column comparisons**: `age > balance`, `price >= cost`
- **Combined simple and complex filters**: `age > 25 AND LENGTH(name) > 5`

**How it works:**

- Simple filters (column-to-constant comparisons like `age > 25`) are handled by TableFilter pushdown, which produces faster MongoDB native queries
- Complex filters (function calls, column-to-column comparisons) are converted to MongoDB `$expr` format and pushed down to MongoDB
- Both simple and complex filters can be combined in a single query, with each handled by the appropriate pushdown mechanism

**Examples:**

```sql
-- Function call filter (complex) - pushed down as $expr
SELECT name, email FROM mongo_test.duckdb_mongo_test.users WHERE LENGTH(name) > 5;
-- MongoDB: {$expr: {$gt: [{$strLenCP: "$name"}, 5]}}

-- Substring filter (complex) - pushed down as $expr
SELECT name FROM mongo_test.duckdb_mongo_test.users WHERE SUBSTRING(name, 1, 3) = 'Ann';
-- MongoDB: {$expr: {$eq: [{$substrCP: ["$name", 0, 3]}, "Ann"]}}

-- Column-to-column comparison (complex) - pushed down as $expr
SELECT name, age, balance FROM mongo_test.duckdb_mongo_test.users WHERE age > balance;
-- MongoDB: {$expr: {$gt: ["$age", "$balance"]}}

-- Combined simple and complex filters
SELECT name FROM mongo_test.duckdb_mongo_test.users WHERE age > 25 AND LENGTH(name) > 5;
-- MongoDB: {age: {$gt: 25}, $expr: {$gt: [{$strLenCP: "$name"}, 5]}}
```

**Use `EXPLAIN` to verify complex filter pushdown:**

```sql
EXPLAIN SELECT name FROM mongo_test.duckdb_mongo_test.users WHERE LENGTH(name) > 5;
```

The plan shows `MONGO_SCAN` directly (no `FILTER` operator above it), indicating the complex filter was pushed down to MongoDB.

> **Note:** Complex filter pushdown works alongside simple filter pushdown. Simple filters are always handled by TableFilter pushdown for optimal performance, while complex filters are handled by `$expr` pushdown when needed.
>
> **Note:** Substring pushdown requires constant start and length arguments (`SUBSTRING(col, start, length)`), with `start >= 1` and `length >= 0`.

#### Semi-Join IN Filter Pushdown

Semi-join IN filter pushdown enables DuckDB to push IN filters from semi-joins (subqueries) to MongoDB as `$in` queries. This optimization works automatically when DuckDB's JoinFilterPushdownOptimizer determines that a semi-join's build side is small enough to push as an IN filter.

**How it works:**

- DuckDB builds a hash table from the subquery (e.g., `SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'`)
- If the hash table is small, DuckDB generates an IN filter with the matching values
- The extension pushes this IN filter to MongoDB as a `$in` query

**Example:**

```sql
-- Query with IN subquery (semi-join)
SELECT ps_suppkey, ps_availqty
FROM partsupp
WHERE ps_partkey IN (
    SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
);
```

The extension automatically pushes the `ps_partkey IN (...)` filter to MongoDB as:

```json
{"ps_partkey": {"$in": [123, 456, 789, ...]}}
```

**Supported Filter Types:**

The extension supports the following DuckDB filter types for pushdown:

| Filter Type | MongoDB Equivalent | Description |
|-------------|-------------------|-------------|
| `CONSTANT_COMPARISON` | `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` | Basic comparisons |
| `IN_FILTER` | `$in` | Value in list |
| `IS_NULL` | `{field: null}` | Null check |
| `IS_NOT_NULL` | `{$ne: null}` | Not null check |
| `CONJUNCTION_AND` | Merged conditions | AND of filters |
| `CONJUNCTION_OR` | `$or` / `$in` | OR of filters |
| `STRUCT_EXTRACT` | Dot notation (`a.b.c`) | Nested field access |
| `OPTIONAL_FILTER` | Unwraps child | Semi-join IN pushdown |
| `DYNAMIC_FILTER` | Unwraps child | Runtime filter pushdown |

#### Aggregation Pushdown

Aggregation pushdown enables pushing `COUNT`, `SUM`, `MIN`, `MAX`, `AVG` aggregates (with optional `GROUP BY`) to MongoDB as aggregation pipelines. This reduces data transfer by computing aggregates server-side rather than fetching all documents to DuckDB.

**Supported Aggregates:**

| Function | MongoDB Pipeline | Notes |
|----------|-----------------|-------|
| `COUNT(*)` | `$count` | Pushed as optimized `$count` stage |
| `COUNT(col)` | `$group` + `$sum` + `$cond` | Counts non-null values |
| `SUM(col)` | `$group` + `$sum` | |
| `MIN(col)` | `$group` + `$min` | |
| `MAX(col)` | `$group` + `$max` | |
| `AVG(col)` | `$group` + `$avg` | |

**Requirements for Aggregation Pushdown:**

- Aggregate functions must use direct column references (no expressions like `SUM(price * quantity)`)
- `GROUP BY` keys must be direct column references
- No `DISTINCT`, `FILTER`, or `ORDER BY` within aggregates
- Single grouping set only (no `GROUPING SETS`, `ROLLUP`, or `CUBE`)

**Examples:**

```sql
-- COUNT(*) pushed down as $count pipeline
SELECT COUNT(*) FROM mongo_test.duckdb_mongo_test.users WHERE active = true;
-- MongoDB pipeline: [{$match: {active: true}}, {$count: "count"}]

-- GROUP BY with aggregates pushed down as $group pipeline
SELECT status, COUNT(*), SUM(total) FROM mongo_test.duckdb_mongo_test.orders GROUP BY status;
-- MongoDB pipeline: [{$group: {_id: {status: "$status"}, __agg0: {$sum: 1}, __agg1: {$sum: "$total"}}}, ...]
```

**Use `EXPLAIN` to verify aggregation pushdown:**

```sql
EXPLAIN SELECT COUNT(*) FROM mongo_test.duckdb_mongo_test.users;
```

The plan shows `MONGO_SCAN` with `scan_method: aggregate` and `pipeline` containing `$count` or `$group`, indicating the aggregation was pushed down to MongoDB.

#### TopN Pushdown

TopN pushdown enables pushing `ORDER BY _id LIMIT N` queries to MongoDB as aggregation pipelines with `$sort` and `$limit` stages. This is particularly efficient for paginated queries ordered by the indexed `_id` field.

**Requirements for TopN Pushdown:**

- Must order by `_id` column only (MongoDB's indexed primary key)
- Must have a `LIMIT` clause (no offset)
- No intermediate operations between `ORDER BY` and the table scan (projections are allowed)

**Example:**

```sql
-- TopN pushed down as $sort + $limit pipeline
SELECT _id, name FROM mongo_test.duckdb_mongo_test.users ORDER BY _id LIMIT 10;
-- MongoDB pipeline: [{$sort: {_id: 1}}, {$limit: 10}]
```

**Use `EXPLAIN` to verify TopN pushdown:**

```sql
EXPLAIN SELECT _id FROM mongo_test.duckdb_mongo_test.users ORDER BY _id LIMIT 5;
```

The plan shows `MONGO_SCAN` with `scan_method: aggregate` and `pipeline` containing `$sort` and `$limit`, indicating the TopN operation was pushed down to MongoDB.

> **Note:** TopN pushdown is conservative and only applies to `ORDER BY _id` queries. This ensures MongoDB can use its indexed `_id` field efficiently. Other ORDER BY columns are processed in DuckDB after fetching data.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

See LICENSE file for details.

### Third-Party Licenses

This project uses the MongoDB C++ Driver (mongocxx and bsoncxx), which is licensed under the Apache License, Version 2.0. See the NOTICE file for third-party license information and attributions.

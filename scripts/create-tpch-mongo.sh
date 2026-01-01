#!/bin/bash
# Script to generate TPC-H data and load it into MongoDB
# Similar to create-postgres-tables.sh but adapted for MongoDB

set -e

DUCKDB_PATH=duckdb
if test -f build/release/duckdb; then
  DUCKDB_PATH=./build/release/duckdb
elif test -f build/reldebug/duckdb; then
  DUCKDB_PATH=./build/reldebug/duckdb
elif test -f build/debug/duckdb; then
  DUCKDB_PATH=./build/debug/duckdb
fi

MONGO_HOST=${MONGO_HOST:-localhost}
MONGO_PORT=${MONGO_PORT:-27017}
MONGO_DB=${MONGO_DB:-tpch}
SCALE_FACTOR=${SCALE_FACTOR:-0.01}

echo "=== Generating TPC-H data (scale factor: $SCALE_FACTOR) ==="

# Create a temporary DuckDB database file to generate and export TPC-H data
TEMP_DB="/tmp/tpch_mongo_tmp/tpch_data.duckdb"
mkdir -p /tmp/tpch_mongo_tmp

# Generate TPC-H data in DuckDB
# First drop schema if it exists from previous run
echo "
DROP SCHEMA IF EXISTS tpch CASCADE;
" | $DUCKDB_PATH $TEMP_DB 2>/dev/null || true

# Generate TPC-H data
# The tpch extension should be available when DuckDB is built with extension support
# Try to load it - if it fails, the error message will guide the user
echo "
LOAD tpch;
CREATE SCHEMA tpch;
CALL dbgen(sf=$SCALE_FACTOR, schema='tpch');
" | $DUCKDB_PATH $TEMP_DB || {
    echo ""
    echo "Error: Failed to load tpch extension."
    echo ""
    echo "The tpch extension needs to be available. Options:"
    echo "1. Rebuild DuckDB with tpch extension: make clean && make"
    echo "2. Or install tpch extension manually (if available online)"
    echo ""
    exit 1
}

echo ""
echo "=== Loading TPC-H data into MongoDB ==="

# Check if mongosh is available
if ! command -v mongosh &> /dev/null; then
    echo "Error: mongosh is not installed. Please install MongoDB shell."
    echo "On macOS: brew install mongosh"
    exit 1
fi

# Drop and create database
mongosh "mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB" --eval "
db.dropDatabase();
print('Database $MONGO_DB dropped and will be recreated');
"

# Export tables to CSV format, then load into MongoDB
TPCH_TABLES=("region" "nation" "supplier" "customer" "part" "partsupp" "orders" "lineitem")

for table in "${TPCH_TABLES[@]}"; do
    echo "Loading $table..."
    
    # Export table to CSV using DuckDB, then load into MongoDB
    CSV_FILE="/tmp/tpch_mongo_tmp/${table}_export.csv"
    
    # Export table to CSV from the temporary database
    # Note: tpch extension is already loaded in TEMP_DB
    echo "
    USE tpch;
    COPY (SELECT * FROM $table) TO '$CSV_FILE' (HEADER, DELIMITER '|');
    " | $DUCKDB_PATH $TEMP_DB
    
    if [ ! -f "$CSV_FILE" ]; then
        echo "Warning: Failed to export $table to CSV, skipping"
        continue
    fi
    
    # Load CSV into MongoDB using mongosh
    mongosh "mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB" --eval "
    const fs = require('fs');
    const csv = fs.readFileSync('$CSV_FILE', 'utf-8');
    const lines = csv.trim().split('\\n');
    
    if (lines.length < 2) {
        print(\"Warning: CSV file for ${table} is empty or has no data rows\");
        quit(0);
    }
    
    // Parse CSV header (first line)
    const headers = lines[0].split(\"|\").map(h => h.trim());
    
    // TPC-H date columns (suffixes that indicate dates)
    const dateSuffixes = [\"date\", \"Date\", \"DATE\"];
    const isDateColumn = (header) => {
        return dateSuffixes.some(suffix => header.toLowerCase().endsWith(suffix.toLowerCase()));
    };
    
    // Helper function to check if a string looks like a date (YYYY-MM-DD format)
    const isDateString = (str) => {
        if (!str || typeof str !== \"string\") return false;
        const datePattern = /^\\d{4}-\\d{2}-\\d{2}$/;
        if (!datePattern.test(str)) return false;
        // Try to parse it as a date
        const date = new Date(str);
        return !isNaN(date.getTime()) && date.toISOString().startsWith(str);
    };
    
    // Convert each row to a MongoDB document
    const documents = [];
    for (let i = 1; i < lines.length; i++) {
        if (!lines[i].trim()) continue;
        const values = lines[i].split('|');
        const doc = {};
        headers.forEach((header, idx) => {
            const value = values[idx];
            if (value === undefined || value === null) {
                doc[header] = null;
            } else {
                // Preserve whitespace to match TPC-H expected results
                // Strip surrounding quotes if present (DuckDB exports strings with special characters quoted)
                let trimmed = value;
                if (trimmed.length >= 2) {
                    if (trimmed.startsWith(\"\\\"\") && trimmed.endsWith(\"\\\"\")) {
                        trimmed = trimmed.slice(1, -1);
                        // Handle escaped quotes (double quotes inside)
                        trimmed = trimmed.replace(/\"\"/g, \"\\\"\");
                    } else if (trimmed.startsWith(\"'\") && trimmed.endsWith(\"'\")) {
                        trimmed = trimmed.slice(1, -1);
                    }
                }
                
                if (trimmed === \"NULL\" || trimmed === \"\") {
                    doc[header] = null;
                } else if (isDateColumn(header) && isDateString(trimmed)) {
                    // Store dates as MongoDB Date objects for proper date comparisons
                    doc[header] = new Date(trimmed);
                } else if (trimmed !== \"\" && !isNaN(trimmed) && trimmed !== \"NULL\") {
                    // Try to convert to number if it looks like a number
                    if (trimmed.includes(\".\")) {
                        doc[header] = parseFloat(trimmed);
                    } else {
                        doc[header] = parseInt(trimmed, 10);
                    }
                } else {
                    doc[header] = trimmed;
                }
            }
        });
        documents.push(doc);
    }
    
    if (documents.length > 0) {
        db.${table}.insertMany(documents);
        print(\"Inserted \" + documents.length + \" documents into ${table}\");
    } else {
        print(\"Warning: No documents to insert for ${table}\");
    }
    "
done

echo ""
echo "=== Creating indexes ==="

# Create indexes similar to what would be in Postgres
mongosh "mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB" --eval "
// Indexes for lineitem (most queried table)
db.lineitem.createIndex({l_orderkey: 1});
db.lineitem.createIndex({l_partkey: 1});
db.lineitem.createIndex({l_suppkey: 1});
db.lineitem.createIndex({l_shipdate: 1});
db.lineitem.createIndex({l_commitdate: 1});
db.lineitem.createIndex({l_receiptdate: 1});

// Indexes for orders
db.orders.createIndex({o_orderkey: 1});
db.orders.createIndex({o_custkey: 1});
db.orders.createIndex({o_orderdate: 1});
db.orders.createIndex({o_orderstatus: 1});

// Indexes for customer
db.customer.createIndex({c_custkey: 1});
db.customer.createIndex({c_nationkey: 1});

// Indexes for part
db.part.createIndex({p_partkey: 1});
db.part.createIndex({p_brand: 1});
db.part.createIndex({p_type: 1});

// Indexes for partsupp
db.partsupp.createIndex({ps_partkey: 1});
db.partsupp.createIndex({ps_suppkey: 1});

// Indexes for supplier
db.supplier.createIndex({s_suppkey: 1});
db.supplier.createIndex({s_nationkey: 1});

// Indexes for nation
db.nation.createIndex({n_nationkey: 1});
db.nation.createIndex({n_regionkey: 1});

// Indexes for region
db.region.createIndex({r_regionkey: 1});

print(\"Indexes created successfully\");
"

# Cleanup temporary files
rm -rf /tmp/tpch_mongo_tmp

echo ""
echo "=== TPC-H data loaded into MongoDB ==="
echo "Database: $MONGO_DB"
echo "Collections: ${TPCH_TABLES[*]}"
echo ""

# Export environment variables for tests
export MONGODB_TEST_DATABASE_AVAILABLE=1
export MONGO_TPCH_DATABASE="$MONGO_DB"

echo "Environment variables set:"
echo "  MONGODB_TEST_DATABASE_AVAILABLE=1"
echo "  MONGO_TPCH_DATABASE=$MONGO_DB"
echo ""
echo "Note: These variables are exported in the current shell session."
echo "To use in other shells, run:"
echo "  export MONGODB_TEST_DATABASE_AVAILABLE=1"
echo "  export MONGO_TPCH_DATABASE=$MONGO_DB"


# TPC-H Benchmarking Guide

This directory contains all files related to TPC-H benchmarking for the DuckDB+MongoDB extension. This guide explains how to set up and run TPC-H benchmarks to compare performance between DuckDB+MongoDB extension and native MongoDB queries.

## Files

- **`benchmark-tpch.sh`** - Main benchmarking script that runs TPC-H queries
- **`benchmark-mongodb-queries.py`** - Python script with MongoDB aggregation pipelines for all 22 TPC-H queries
- **`create-tpch-mongo.sh`** - Script to generate TPC-H data and load it into MongoDB

## Overview

The benchmarking suite allows you to:
- Run TPC-H queries using DuckDB+MongoDB extension (SQL interface)
- Run equivalent queries using native MongoDB aggregation pipelines
- Compare performance between both approaches
- Analyze results to understand when each approach is optimal

## Prerequisites

Before running benchmarks, ensure you have:

1. **DuckDB Built**: Build DuckDB with the MongoDB extension
   ```bash
   cd /path/to/duckdb-mongo
   make release
   ```

2. **MongoDB Running**: Local MongoDB instance on default port (27017)
   ```bash
   mongod --dbpath /path/to/data
   ```

3. **Python Dependencies**: Install pymongo for native MongoDB queries
   ```bash
   pip install pymongo
   ```

4. **TPC-H Data Loaded**: Generate and load TPC-H data into MongoDB
   ```bash
   cd benchmarks
   ./create-tpch-mongo.sh
   ```

## Quick Start

### Basic Usage

```bash
cd benchmarks

# Run all queries in summary mode (recommended first run)
./benchmark-tpch.sh all 3

# Run all queries with detailed comparison
./benchmark-tpch.sh all 3 --verbose

# Run a specific query
./benchmark-tpch.sh 1 5

# Run a specific query with verbose output
./benchmark-tpch.sh 1 5 --verbose
```

### Command Syntax

```bash
./benchmark-tpch.sh [query_number|all] [iterations] [--verbose|-v]

Arguments:
  query_number  - Query number (1-22) or 'all' for all queries
  iterations    - Number of times to run each query (default: 3)
  --verbose|-v  - Enable detailed output mode
```

## Setting Up TPC-H Data

### Step 1: Generate and Load Data

The `create-tpch-mongo.sh` script handles the entire setup process:

```bash
cd benchmarks
./create-tpch-mongo.sh
```

This script will:
1. Generate TPC-H data using DuckDB's `dbgen()` function (scale factor 0.01)
2. Export each table to CSV format
3. Load CSV data into MongoDB collections
4. Convert date strings to MongoDB Date objects
5. Create appropriate indexes for TPC-H queries
6. Set environment variables for testing

### Step 2: Verify Setup

Check that data was loaded correctly:

```bash
mongosh tpch --eval "db.lineitem.countDocuments()"
mongosh tpch --eval "db.orders.countDocuments()"
mongosh tpch --eval "db.getCollectionNames()"
```

You should see document counts and a list of collections (customer, lineitem, nation, orders, part, partsupp, region, supplier).

### Step 3: Environment Variables

The script exports these environment variables (source the script to use them):
- `MONGODB_TEST_DATABASE_AVAILABLE=1`
- `MONGO_TPCH_DATABASE=tpch`

## Understanding the Benchmark

### What Gets Compared

**DuckDB+MongoDB Extension:**
- Executes SQL queries via `PRAGMA tpch(N)`
- Queries MongoDB collections through DuckDB's query engine
- Benefits from DuckDB's query optimization
- Provides SQL interface for MongoDB data

**Native MongoDB:**
- Uses MongoDB aggregation pipelines
- Direct queries to MongoDB
- Maximum performance for MongoDB-native operations
- Requires MongoDB aggregation syntax knowledge

### TPC-H Benchmark

TPC-H is a standard decision support benchmark with 22 analytical queries:
- **Query Types**: Aggregations, joins, subqueries, date operations, grouping
- **Complexity**: Ranges from simple filters to complex multi-table joins
- **Data Model**: 8 tables simulating a parts supplier database
- **Scale Factor**: Default is 0.01 (SF-0.01, ~1MB data)

## Output Modes

### Summary Mode (Default)

Summary mode provides a quick overview:
- Average execution time per query
- Min/Max times across iterations
- Overall statistics (total time, fastest/slowest queries)
- Queries sorted by speed

**Best for:** Quick performance overview, identifying slow queries

### Verbose Mode (`--verbose` or `-v`)

Verbose mode provides detailed analysis:
- Individual run times for each iteration
- Direct comparison between DuckDB+MongoDB and native MongoDB
- Speedup/slowdown ratios
- Detailed timing breakdown per query

**Best for:** Detailed analysis, debugging, understanding performance characteristics

## Understanding Results

### Execution Time

Times are measured in milliseconds (ms) and include:
- Query execution time
- Result fetching time
- Network overhead (minimal for localhost)

### Consistency

Good consistency (low variance between min/max) indicates:
- Stable performance
- Minimal interference from other processes
- Reliable benchmark results

### Performance Comparison

When comparing DuckDB+MongoDB vs native MongoDB:
- **Speedup > 1x**: Native MongoDB is faster
- **Speedup < 1x**: DuckDB+MongoDB extension is faster
- **Large gaps**: May indicate optimization opportunities

## Interpreting Results

### Simple Queries (< 100ms)
- Typically show larger overhead from SQL layer
- Native MongoDB often 2-12x faster
- Overhead more noticeable on simple operations

### Moderate Queries (100-300ms)
- Performance gap varies by query pattern
- Some queries benefit from DuckDB's optimizer
- Gap typically 1.4-5x

### Complex Queries (> 300ms)
- DuckDB's optimizer can provide significant benefits
- Complex EXISTS/NOT EXISTS conditions may favor DuckDB
- Extension can outperform native MongoDB on complex queries

## Tips for Accurate Benchmarks

1. **Multiple Iterations**: Use 3-5 iterations for reliable averages
   ```bash
   ./benchmark-tpch.sh all 5
   ```

2. **Warm-up**: First run may be slower due to cold start
   - Consider discarding first iteration
   - Or run a warm-up query before benchmarking

3. **System Load**: Run benchmarks on idle system
   - Close unnecessary applications
   - Avoid running other database workloads

4. **Index Verification**: Ensure MongoDB indexes exist
   - Created automatically by `create-tpch-mongo.sh`
   - Verify with: `mongosh tpch --eval "db.lineitem.getIndexes()"`

5. **Result Validation**: Verify both approaches return identical results
   - Compare output manually for a few queries
   - Ensure data consistency

## Troubleshooting

### MongoDB Connection Issues

```bash
# Check if MongoDB is running
mongosh --eval "db.adminCommand('ping')"

# Verify connection string
mongosh "mongodb://localhost:27017/tpch"
```

### DuckDB Not Found

```bash
# Ensure DuckDB is built
make release

# Check if binary exists
ls -lh build/release/duckdb
```

### Python Script Errors

```bash
# Verify pymongo is installed
python3 -c "import pymongo; print(pymongo.__version__)"

# Check Python script syntax
python3 -m py_compile benchmark-mongodb-queries.py
```

### Missing TPC-H Data

```bash
# Re-run data setup
./create-tpch-mongo.sh

# Verify collections exist
mongosh tpch --eval "db.getCollectionNames()"
```

## Advanced Usage

### Custom Scale Factor

Edit `create-tpch-mongo.sh` to change scale factor:
```bash
# Change this line in create-tpch-mongo.sh
CALL dbgen(sf=0.01, schema='tpch');
# To:
CALL dbgen(sf=0.1, schema='tpch');  # 10x more data
```

### Running Specific Query Range

```bash
# Run queries 1-10
for i in {1..10}; do
  ./benchmark-tpch.sh $i 3 --verbose
done
```

### Exporting Results

```bash
# Save results to file
./benchmark-tpch.sh all 3 > results.txt

# Save verbose results
./benchmark-tpch.sh all 3 --verbose > verbose_results.txt
```

## Next Steps

After running benchmarks:

1. **Analyze Results**: Identify which queries are slowest
2. **Compare Approaches**: See where each approach excels
3. **Optimize**: Focus on slow queries or large performance gaps
4. **Scale Testing**: Run with larger scale factors for production-like workloads
5. **Profile**: Use MongoDB profiler or DuckDB explain plans for detailed analysis

## Additional Resources

- **TPC-H Specification**: [TPC-H Benchmark](http://www.tpc.org/tpch/)
- **DuckDB Documentation**: [DuckDB Docs](https://duckdb.org/docs/)
- **MongoDB Aggregation**: [MongoDB Aggregation Framework](https://www.mongodb.com/docs/manual/aggregation/)
- **MongoDB Indexes**: [MongoDB Indexing](https://www.mongodb.com/docs/manual/indexes/)

---

*This guide helps you set up and run TPC-H benchmarks. For detailed results and analysis, run the benchmarks and analyze the output.*

#!/bin/bash
# Benchmark script for TPC-H queries with DuckDB+MongoDB extension
# Usage: 
#   ./benchmarks/benchmark-tpch.sh [query_number|all] [iterations] [--verbose] [--show-results]
#   ./benchmarks/benchmark-tpch.sh all 3              # Summary mode (default)
#   ./benchmarks/benchmark-tpch.sh all 3 --verbose    # Detailed comparison mode
#   ./benchmarks/benchmark-tpch.sh 6 1 --show-results # Run query once and show results
#   ./benchmarks/benchmark-tpch.sh 6 3 --verbose --show-results # Show results then benchmark comparison

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# Python script is in the same directory as this script
DUCKDB_PATH="${PROJECT_ROOT}/build/release/duckdb"
MONGO_DB="${MONGO_TPCH_DATABASE:-tpch}"
MONGO_HOST="${MONGO_HOST:-localhost}"
MONGO_PORT="${MONGO_PORT:-27017}"

# Parse arguments
QUERY_NUM="all"
ITERATIONS=3
VERBOSE=false
SHOW_RESULTS=false
QUERY_SET=false

for arg in "$@"; do
    case $arg in
        --verbose|-v)
            VERBOSE=true
            ;;
        --show-results|-s)
            SHOW_RESULTS=true
            ;;
        [0-9]*)
            if [ "$QUERY_SET" = false ] && [ "$arg" -ge 1 ] && [ "$arg" -le 22 ]; then
                QUERY_NUM=$arg
                QUERY_SET=true
            elif [ "$QUERY_SET" = true ] && [ "$arg" -ge 1 ]; then
                ITERATIONS=$arg
            fi
            ;;
        all)
            QUERY_NUM="all"
            QUERY_SET=true
            ;;
    esac
done

if [ ! -f "$DUCKDB_PATH" ]; then
    echo "Error: DuckDB binary not found at $DUCKDB_PATH"
    echo "Please build DuckDB first: make release"
    exit 1
fi

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Arrays to store results (for summary mode)
declare -a query_times
declare -a query_names

# Function to warm up connection and schema (runs once before benchmarks)
warmup_connection() {
    # Run a simple query to warm up MongoDB connection, schema inference, and query plan cache
    "$DUCKDB_PATH" -c "
        ATTACH 'host=$MONGO_HOST port=$MONGO_PORT database=$MONGO_DB' AS tpch_mongo (TYPE MONGO);
        SET search_path='tpch_mongo.tpch';
        SELECT COUNT(*) FROM lineitem LIMIT 1;
    " > /dev/null 2>&1
}

# Function to run DuckDB query and measure time
run_duckdb_query() {
    local query_num=$1
    local temp_file=$(mktemp)
    
    echo "PRAGMA tpch($query_num);" > "$temp_file"
    
    local start_time=$(date +%s.%N)
    # Always suppress output for benchmarking (--show-results is handled separately)
    "$DUCKDB_PATH" -c "
        ATTACH 'host=$MONGO_HOST port=$MONGO_PORT database=$MONGO_DB' AS tpch_mongo (TYPE MONGO);
        SET search_path='tpch_mongo.tpch';
        $(cat "$temp_file")
    " > /dev/null 2>&1
    local end_time=$(date +%s.%N)
    
    rm "$temp_file"
    
    # Calculate duration in milliseconds
    local duration=$(echo "($end_time - $start_time) * 1000" | bc)
    echo "$duration"
}

# Function to run native MongoDB query using Python script
run_mongodb_query() {
    local query_num=$1
    local python_script="${SCRIPT_DIR}/benchmark-mongodb-queries.py"
    
    if [ ! -f "$python_script" ]; then
        echo "0"
        return
    fi
    
    # Check if pymongo is available
    if ! python3 -c "import pymongo" 2>/dev/null; then
        echo "0"
        return
    fi
    
    # Run query and capture both stdout and stderr
    local duration=$(python3 "$python_script" "$MONGO_HOST" "$MONGO_PORT" "$MONGO_DB" "$query_num" 2>&1)
    
    # Check if query is implemented (returns > 0) or not (returns 0 or error)
    if [ -z "$duration" ] || [ "$duration" = "0" ] || [ "$duration" = "0.00" ]; then
        echo "0"
    else
        echo "$duration"
    fi
}

# Function to format query name
format_query_name() {
    local num=$1
    printf "Q%02d" "$num"
}

# Function to benchmark a single query (verbose mode)
benchmark_query_verbose() {
    local query_num=$1
    local query_name=$(format_query_name "$query_num")
    
    echo -e "${BLUE}=== $query_name ===${NC}"
    
    # DuckDB benchmarks
    echo -e "${YELLOW}Running DuckDB+MongoDB extension...${NC}"
    local duckdb_times=()
    for i in $(seq 1 $ITERATIONS); do
        local time=$(run_duckdb_query "$query_num")
        duckdb_times+=("$time")
        printf "  Run %d: %.2f ms\n" "$i" "$time"
    done
    
    # Calculate DuckDB statistics
    local duckdb_avg=$(printf '%s\n' "${duckdb_times[@]}" | awk '{sum+=$1; count++} END {print sum/count}')
    local duckdb_min=$(printf '%s\n' "${duckdb_times[@]}" | sort -n | head -1)
    local duckdb_max=$(printf '%s\n' "${duckdb_times[@]}" | sort -n | tail -1)
    
    echo -e "${GREEN}DuckDB+MongoDB:${NC}"
    echo "  Average: $(printf "%.2f" "$duckdb_avg") ms"
    echo "  Min:     $(printf "%.2f" "$duckdb_min") ms"
    echo "  Max:     $(printf "%.2f" "$duckdb_max") ms"
    
    # MongoDB benchmarks
    echo -e "${YELLOW}Running native MongoDB...${NC}"
    local mongodb_times=()
    local mongodb_implemented=false
    
    for i in $(seq 1 $ITERATIONS); do
        local time=$(run_mongodb_query "$query_num")
        if [ "$(echo "$time > 0" | bc)" -eq 1 ]; then
            mongodb_times+=("$time")
            mongodb_implemented=true
            printf "  Run %d: %.2f ms\n" "$i" "$time"
        fi
    done
    
    if [ "$mongodb_implemented" = true ] && [ ${#mongodb_times[@]} -gt 0 ]; then
        local mongodb_avg=$(printf '%s\n' "${mongodb_times[@]}" | awk '{sum+=$1; count++} END {print sum/count}')
        local mongodb_min=$(printf '%s\n' "${mongodb_times[@]}" | sort -n | head -1)
        local mongodb_max=$(printf '%s\n' "${mongodb_times[@]}" | sort -n | tail -1)
        
        echo -e "${GREEN}MongoDB:${NC}"
        echo "  Average: $(printf "%.2f" "$mongodb_avg") ms"
        echo "  Min:     $(printf "%.2f" "$mongodb_min") ms"
        echo "  Max:     $(printf "%.2f" "$mongodb_max") ms"
        
        # Calculate speedup
        local speedup=$(echo "scale=2; $mongodb_avg / $duckdb_avg" | bc)
        if [ "$(echo "$speedup > 1" | bc)" -eq 1 ]; then
            echo -e "${GREEN}Speedup: ${speedup}x faster with DuckDB+MongoDB${NC}"
        else
            local slowdown=$(echo "scale=2; 1 / $speedup" | bc)
            echo -e "${YELLOW}MongoDB is ${slowdown}x faster${NC}"
        fi
    else
        echo -e "${GREEN}MongoDB:${NC}"
        echo "  (Not implemented yet - requires MongoDB aggregation queries)"
    fi
    
    echo ""
}

# Function to benchmark a single query (summary mode)
benchmark_query_summary() {
    local query_num=$1
    local query_name=$(format_query_name "$query_num")
    
    printf "%-4s " "$query_name"
    
    local times=()
    for i in $(seq 1 $ITERATIONS); do
        local time=$(run_duckdb_query "$query_num")
        times+=("$time")
    done
    
    # Calculate average
    local avg=$(printf '%s\n' "${times[@]}" | awk '{sum+=$1; count++} END {print sum/count}')
    local min=$(printf '%s\n' "${times[@]}" | sort -n | head -1)
    local max=$(printf '%s\n' "${times[@]}" | sort -n | tail -1)
    
    query_times+=("$avg")
    query_names+=("$query_name")
    
    printf "Avg: %7.2f ms  (Min: %7.2f, Max: %7.2f)\n" "$avg" "$min" "$max"
}

# Function to print summary statistics
print_summary() {
    echo ""
    echo "=== Summary Statistics ==="
    
    # Calculate total time
    local total=0
    for time in "${query_times[@]}"; do
        total=$(echo "$total + $time" | bc)
    done
    
    # Find fastest and slowest
    local fastest_idx=0
    local slowest_idx=0
    local fastest_time=${query_times[0]}
    local slowest_time=${query_times[0]}
    
    for i in "${!query_times[@]}"; do
        if (( $(echo "${query_times[$i]} < $fastest_time" | bc -l) )); then
            fastest_time=${query_times[$i]}
            fastest_idx=$i
        fi
        if (( $(echo "${query_times[$i]} > $slowest_time" | bc -l) )); then
            slowest_time=${query_times[$i]}
            slowest_idx=$i
        fi
    done
    
    echo "Total time (all queries): $(printf "%.2f" "$total") ms"
    echo "Average per query: $(printf "%.2f" "$(echo "scale=2; $total / ${#query_times[@]}" | bc)") ms"
    echo "Fastest query: ${query_names[$fastest_idx]} ($(printf "%.2f" "$fastest_time") ms)"
    echo "Slowest query: ${query_names[$slowest_idx]} ($(printf "%.2f" "$slowest_time") ms)"
    echo ""
    
    # Show queries sorted by speed
    echo "=== Queries Sorted by Speed (Fastest to Slowest) ==="
    for i in "${!query_times[@]}"; do
        printf "%s:%.2f\n" "${query_names[$i]}" "${query_times[$i]}"
    done | sort -t: -k2 -n | while IFS=: read name time; do
        printf "%-4s %7.2f ms\n" "$name" "$time"
    done
}

# Function to estimate scale factor from MongoDB data
estimate_scale_factor() {
    local lineitem_count=$(mongosh "mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB" --quiet --eval "db.lineitem.countDocuments()" 2>/dev/null || echo "0")
    if [ "$lineitem_count" -gt 5000000 ]; then
        echo "1"
    elif [ "$lineitem_count" -gt 500000 ]; then
        echo "0.1"
    elif [ "$lineitem_count" -gt 50000 ]; then
        echo "0.01"
    else
        echo "unknown"
    fi
}

# Main execution
SF_ESTIMATE=$(estimate_scale_factor)

# If --show-results is set, show query results first
if [ "$SHOW_RESULTS" = true ]; then
    if [ "$QUERY_NUM" = "all" ]; then
        echo "Error: --show-results can only be used with a specific query number (1-22)"
        exit 1
    fi
    
    echo "Running TPC-H Query $QUERY_NUM..."
    echo "MongoDB: mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB"
    echo "Scale Factor: SF-$SF_ESTIMATE (estimated from data size)"
    echo ""
    
    start_time=$(date +%s.%N)
    "$DUCKDB_PATH" -c "
        ATTACH 'host=$MONGO_HOST port=$MONGO_PORT database=$MONGO_DB' AS tpch_mongo (TYPE MONGO);
        SET search_path='tpch_mongo.tpch';
        PRAGMA tpch($QUERY_NUM);
    "
    end_time=$(date +%s.%N)
    
    duration=$(echo "($end_time - $start_time) * 1000" | bc)
    echo ""
    echo "Query completed in: $(printf "%.2f" "$duration") ms"
    
    # If --verbose is also set, continue with benchmark comparison
    if [ "$VERBOSE" = false ]; then
        exit 0
    else
        echo ""
        echo "=== Benchmark Comparison ==="
        echo ""
    fi
fi

if [ "$VERBOSE" = true ]; then
    echo "=== TPC-H Benchmark Comparison ==="
    echo "Database: $MONGO_DB"
    echo "Host: $MONGO_HOST:$MONGO_PORT"
    echo "Scale Factor: SF-$SF_ESTIMATE (estimated from data size)"
    echo "Iterations: $ITERATIONS"
    echo ""
    echo -e "${YELLOW}Warming up connection and schema...${NC}"
    warmup_connection
    echo -e "${GREEN}Warmup complete.${NC}"
    echo ""
    
    if [ "$QUERY_NUM" = "all" ]; then
        echo "Running all TPC-H queries (1-22)..."
        echo ""
        
        for q in {1..22}; do
            benchmark_query_verbose "$q"
        done
        
        echo "=== Summary ==="
        echo "All queries completed. See individual results above."
    else
        benchmark_query_verbose "$QUERY_NUM"
    fi
else
    echo "=== TPC-H Performance Summary (DuckDB+MongoDB Extension) ==="
    echo "Database: $MONGO_DB | Host: $MONGO_HOST:$MONGO_PORT | Scale Factor: SF-$SF_ESTIMATE | Iterations: $ITERATIONS"
    echo ""
    echo "Warming up connection and schema..."
    warmup_connection
    echo "Warmup complete."
    echo ""
    
    if [ "$QUERY_NUM" = "all" ]; then
        echo "Query | Average Time (ms) | Min | Max"
        echo "------|-------------------|-----|-----"
        
        for q in {1..22}; do
            benchmark_query_summary "$q"
        done
        
        print_summary
    else
        echo "Query | Average Time (ms) | Min | Max"
        echo "------|-------------------|-----|-----"
        benchmark_query_summary "$QUERY_NUM"
        print_summary
    fi
fi

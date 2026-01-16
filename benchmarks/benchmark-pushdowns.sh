#!/bin/bash
# Benchmark targeted pushdowns (COUNT / GROUP BY / TopN) and a join-oriented query.
#
# Usage:
#   ./benchmarks/benchmark-pushdowns.sh [iterations]
#
# Env vars:
#   DUCKDB_PATH   - path to DuckDB binary (default: build/release/duckdb)
#   MONGO_ATTACH  - attach string for ATTACH ... (TYPE MONGO)
#                  default: "host=localhost port=27017 database=tpch"
#
# Notes:
# - Run this script on the base branch and again on your PR branch for before/after comparison.
# - Output is written to benchmarks/results/pushdowns-<timestamp>.csv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ITERATIONS="${1:-5}"
DUCKDB_PATH="${DUCKDB_PATH:-${PROJECT_ROOT}/build/release/duckdb}"
MONGO_ATTACH="${MONGO_ATTACH:-host=localhost port=27017 database=tpch}"

QUERIES_FILE="${SCRIPT_DIR}/pushdown_queries.sql"
OUT_DIR="${SCRIPT_DIR}/results"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_CSV="${OUT_DIR}/pushdowns-${TS}.csv"

mkdir -p "${OUT_DIR}"

if [ ! -f "${DUCKDB_PATH}" ]; then
  echo "Error: DuckDB binary not found at ${DUCKDB_PATH}"
  echo "Build it with: make release"
  exit 1
fi

if [ ! -f "${QUERIES_FILE}" ]; then
  echo "Error: queries file not found at ${QUERIES_FILE}"
  exit 1
fi

echo "name,iteration,ms" > "${OUT_CSV}"

extract_queries() {
  # Extracts blocks of SQL following '-- name: <name>' until the next '-- name:' or EOF.
  # Prints: name<TAB>sql
  awk '
    BEGIN { name=""; sql=""; }
    /^-- name: / {
      if (name != "") {
        gsub(/\n+$/, "", sql);
        print name "\t" sql;
      }
      name = substr($0, 10);
      sql = "";
      next;
    }
    { sql = sql $0 "\n"; }
    END {
      if (name != "") {
        gsub(/\n+$/, "", sql);
        print name "\t" sql;
      }
    }
  ' "${QUERIES_FILE}"
}

run_query_ms() {
  local sql="$1"
  local start end
  start="$(date +%s.%N)"
  "${DUCKDB_PATH}" -c "
    ATTACH '${MONGO_ATTACH}' AS tpch_mongo (TYPE MONGO);
    SET search_path='tpch_mongo.tpch';
    ${sql}
  " > /dev/null 2>&1
  end="$(date +%s.%N)"
  echo "(${end} - ${start}) * 1000" | bc
}

echo "Benchmarking pushdowns (${ITERATIONS} iterations each)"
echo "DuckDB: ${DUCKDB_PATH}"
echo "MONGO_ATTACH: ${MONGO_ATTACH}"
echo "Output: ${OUT_CSV}"
echo ""

# Warm up connection and schema
"${DUCKDB_PATH}" -c "
  ATTACH '${MONGO_ATTACH}' AS tpch_mongo (TYPE MONGO);
  SET search_path='tpch_mongo.tpch';
  SELECT COUNT(*) FROM lineitem LIMIT 1;
" > /dev/null 2>&1 || true

while IFS=$'\t' read -r name sql; do
  # Warm up each query
  run_query_ms "${sql}" > /dev/null || true
  for i in $(seq 1 "${ITERATIONS}"); do
    ms="$(run_query_ms "${sql}")"
    printf "%s,%d,%.2f\n" "${name}" "${i}" "${ms}" | tee -a "${OUT_CSV}" > /dev/null
  done
done < <(extract_queries)

echo ""
echo "Done. Results in ${OUT_CSV}"


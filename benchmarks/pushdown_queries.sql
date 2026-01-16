-- Queries used by benchmarks/benchmark-pushdowns.sh
-- Assumes:
--   ATTACH '${MONGO_ATTACH}' AS tpch_mongo (TYPE MONGO);
--   SET search_path='tpch_mongo.tpch';

-- COUNT pushdown
-- name: count_filtered
SELECT COUNT(*)
FROM lineitem
WHERE l_shipdate >= DATE '1996-01-01';

-- GROUP BY pushdown
-- name: groupby_returnflag
SELECT l_returnflag, COUNT(*) AS c, SUM(l_extendedprice) AS s
FROM lineitem
WHERE l_shipdate >= DATE '1996-01-01'
GROUP BY l_returnflag;

-- TopN pushdown (conservative: ORDER BY _id only)
-- name: topn_id
SELECT _id
FROM lineitem
ORDER BY _id
LIMIT 1000;

-- Join-oriented benchmark: join pushed-down aggregate to a small local CSV dimension
-- name: join_agg_to_csv
WITH agg AS (
  SELECT l_returnflag, SUM(l_extendedprice) AS s
  FROM lineitem
  GROUP BY l_returnflag
)
SELECT a.l_returnflag, d.description, a.s
FROM agg a
JOIN read_csv_auto('benchmarks/data/returnflag_dim.csv') d
USING (l_returnflag)
ORDER BY a.l_returnflag;


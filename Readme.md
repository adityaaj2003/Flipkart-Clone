WITH latest_year AS (
  SELECT EXTRACT(YEAR FROM MAX(o_orderdate))::int AS y
  FROM orders
),
cust_months AS (
  SELECT
      o_custkey AS custkey,
      EXTRACT(MONTH FROM o_orderdate)::int AS mon
  FROM orders o
  JOIN latest_year ly
    ON EXTRACT(YEAR FROM o.o_orderdate) = ly.y
  GROUP BY o_custkey, EXTRACT(MONTH FROM o_orderdate)
),
qualified AS (
  SELECT custkey
  FROM cust_months
  GROUP BY custkey
  HAVING COUNT(DISTINCT mon) = 12
)
SELECT c.c_name
FROM customer c
JOIN qualified q
  ON q.custkey = c.c_custkey
ORDER BY c.c_name;

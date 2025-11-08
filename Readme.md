WITH multi_brand_parts AS (
  SELECT
    p.partkey
  FROM part p
  GROUP BY p.partkey
  HAVING COUNT(DISTINCT p.brand) >= 2
),
supplier_rev AS (
  SELECT
    l.suppkey AS supplier_id,
    SUM(l.extendedprice * (1 - l.discount)) AS discounted_revenue
  FROM lineitem l
  JOIN part p ON p.partkey = l.partkey
  JOIN multi_brand_parts mb ON mb.partkey = p.partkey
  GROUP BY l.suppkey
)
SELECT supplier_id, discounted_revenue
FROM supplier_rev
ORDER BY supplier_id;

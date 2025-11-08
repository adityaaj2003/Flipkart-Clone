-- Orders above their priority's average total price
-- and with at least one late lineitem
WITH avg_by_priority AS (
  SELECT
    o.orderpriority,
    AVG(o.totalprice) AS avg_totalprice
  FROM orders o
  GROUP BY o.orderpriority
)
SELECT
  o.orderkey,
  o.orderdate,
  o.totalprice
FROM orders o
JOIN avg_by_priority a
  ON a.orderpriority = o.orderpriority
WHERE o.totalprice > a.avg_totalprice
  AND EXISTS (
        SELECT 1
        FROM lineitem l
        WHERE l.orderkey = o.orderkey
          AND l.receiptdate > l.commitdate
      )
ORDER BY o.orderdate, o.orderkey;

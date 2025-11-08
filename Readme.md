WITH first_orders AS (
  SELECT
    c.custkey,
    c.name AS customer_name,
    o.orderkey,
    o.orderdate,
    ROW_NUMBER() OVER (PARTITION BY c.custkey ORDER BY o.orderdate) AS rn
  FROM customer c
  JOIN orders o ON o.custkey = c.custkey
)
SELECT
  f.customer_name,
  f.orderdate AS first_order_date
FROM first_orders f
WHERE f.rn = 1
  AND EXISTS (
        SELECT 1
        FROM lineitem l
        WHERE l.orderkey = f.orderkey
          AND l.shipmode = 'AIR'
      )
ORDER BY f.customer_name;

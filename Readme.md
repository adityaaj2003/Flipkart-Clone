WITH earliest AS (
    SELECT
        o_custkey,
        MIN(o_orderdate) AS earliest_date
    FROM orders
    GROUP BY o_custkey
),
latest AS (
    SELECT
        o_custkey,
        MAX(o_orderdate) AS latest_date
    FROM orders
    GROUP BY o_custkey
)
SELECT
    c.c_name,
    e.earliest_date AS earliest_order_date,
    o1.o_totalprice AS earliest_order_price,
    l.latest_date AS latest_order_date,
    o2.o_totalprice AS latest_order_price
FROM customer c
JOIN earliest e
  ON c.c_custkey = e.o_custkey
JOIN latest l
  ON c.c_custkey = l.o_custkey
JOIN orders o1
  ON o1.o_custkey = c.c_custkey AND o1.o_orderdate = e.earliest_date
JOIN orders o2
  ON o2.o_custkey = c.c_custkey AND o2.o_orderdate = l.latest_date
ORDER BY c.c_name;

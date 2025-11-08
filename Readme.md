WITH first_order AS (
  SELECT
      o_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      ROW_NUMBER() OVER (
        PARTITION BY o_custkey
        ORDER BY o_orderdate ASC, o_orderkey ASC
      ) AS rn
  FROM orders
),
last_order AS (
  SELECT
      o_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      ROW_NUMBER() OVER (
        PARTITION BY o_custkey
        ORDER BY o_orderdate DESC, o_orderkey DESC
      ) AS rn
  FROM orders
)
SELECT
    c.c_name,
    f.o_orderdate  AS earliest_order_date,
    f.o_totalprice AS earliest_order_price,
    l.o_orderdate  AS latest_order_date,
    l.o_totalprice AS latest_order_price
FROM customer c
LEFT JOIN first_order f
  ON f.o_custkey = c.c_custkey AND f.rn = 1
LEFT JOIN last_order l
  ON l.o_custkey = c.c_custkey AND l.rn = 1
ORDER BY c.c_name;

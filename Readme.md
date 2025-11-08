WITH order_revenue AS (
  SELECT
      o.o_orderkey,
      o.o_custkey,
      o.o_orderdate,
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS order_revenue
  FROM orders o
  JOIN lineitem l
    ON l.l_orderkey = o.o_orderkey
  GROUP BY o.o_orderkey, o.o_custkey, o.o_orderdate
)
SELECT
    c.c_custkey        AS custkey,
    c.c_name           AS customer_name,
    r.o_orderdate      AS order_date,
    r.order_revenue    AS order_revenue,
    SUM(r.order_revenue) OVER (
        PARTITION BY r.o_custkey
        ORDER BY r.o_orderdate, r.o_orderkey
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                  AS cumulative_revenue
FROM order_revenue r
JOIN customer c
  ON c.c_custkey = r.o_custkey
ORDER BY c.c_custkey, r.o_orderdate, r.o_orderkey;

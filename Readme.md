WITH max_discounts AS (
    SELECT
        l_orderkey,
        MAX(l_discount) AS max_discount
    FROM lineitem
    GROUP BY l_orderkey
)
SELECT
    o.o_orderkey,
    s.s_suppkey,
    s.s_name,
    md.max_discount AS highest_discount
FROM max_discounts md
JOIN lineitem l
  ON l.l_orderkey = md.l_orderkey
 AND l.l_discount = md.max_discount
JOIN supplier s
  ON s.s_suppkey = l.l_suppkey
JOIN orders o
  ON o.o_orderkey = l.l_orderkey
ORDER BY o.o_orderkey;

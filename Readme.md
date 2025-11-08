WITH ranked_sales AS (
  SELECT
    l.suppkey,
    l.orderkey,
    l.linenumber,
    l.extendedprice,
    DENSE_RANK() OVER (
      PARTITION BY l.suppkey
      ORDER BY l.extendedprice DESC
    ) AS rnk
  FROM lineitem l
)
SELECT
  suppkey AS supplier_id,
  orderkey,
  linenumber,
  extendedprice AS largest_single_sale
FROM ranked_sales
WHERE rnk = 1
ORDER BY suppkey, orderkey, linenumber;

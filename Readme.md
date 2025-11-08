WITH cs_counts AS (
  SELECT
    o.custkey                  AS c_custkey,
    l.suppkey                  AS supplier_id,
    COUNT(*)                   AS frequency
  FROM orders   o
  JOIN lineitem l ON l.orderkey = o.orderkey
  GROUP BY o.custkey, l.suppkey
),
ranked AS (
  SELECT
    c_custkey, supplier_id, frequency,
    DENSE_RANK() OVER (PARTITION BY c_custkey ORDER BY frequency DESC) AS rnk
  FROM cs_counts
)
SELECT
  c_custkey,
  supplier_id,
  frequency
FROM ranked
WHERE rnk = 1
ORDER BY c_custkey, supplier_id;






from pyspark.sql import functions as F
from pyspark.sql.window import Window

# counts per (customer, supplier)
cs_counts = (
    orders.alias("o")
    .join(lineitem.alias("l"), F.col("l.orderkey") == F.col("o.orderkey"))
    .groupBy(F.col("o.custkey").alias("c_custkey"),
             F.col("l.suppkey").alias("supplier_id"))
    .agg(F.count(F.lit(1)).alias("frequency"))
)

# rank within each customer by descending frequency
w = Window.partitionBy("c_custkey").orderBy(F.col("frequency").desc())

top_suppliers = (
    cs_counts
    .withColumn("rnk", F.dense_rank().over(w))  # use row_number() for single winner
    .where(F.col("rnk") == 1)
    .select("c_custkey", "supplier_id", "frequency")
    .orderBy("c_custkey", "supplier_id")
)

WITH cust_revenue AS (
  SELECT
    c.custkey,
    r.regionkey,
    r.name AS region,
    SUM(l.extendedprice * (1 - l.discount)) AS revenue
  FROM customer c
  JOIN nation   n ON n.nationkey  = c.nationkey
  JOIN region   r ON r.regionkey   = n.regionkey
  JOIN orders   o ON o.custkey     = c.custkey
  JOIN lineitem l ON l.orderkey    = o.orderkey
  GROUP BY c.custkey, r.regionkey, r.name
)
SELECT
  custkey,
  region,
  revenue,
  /* 0 = lowest in region, 100 = highest in region (if >1 row); single-row region -> 0 */
  PERCENT_RANK() OVER (PARTITION BY regionkey ORDER BY revenue) * 100 AS percentile_rank,
  /* Optional: cumulative % of customers at or below this revenue */
  CUME_DIST()    OVER (PARTITION BY regionkey ORDER BY revenue) * 100 AS cume_percent
FROM cust_revenue
ORDER BY region, revenue;






from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1) Total revenue per customer with region
cust_revenue = (
    customer.alias("c")
    .join(nation.alias("n"), F.col("n.nationkey") == F.col("c.nationkey"))
    .join(region.alias("r"), F.col("r.regionkey") == F.col("n.regionkey"))
    .join(orders.alias("o"), F.col("o.custkey") == F.col("c.custkey"))
    .join(lineitem.alias("l"), F.col("l.orderkey") == F.col("o.orderkey"))
    .groupBy(F.col("c.custkey").alias("custkey"),
             F.col("r.regionkey").alias("regionkey"),
             F.col("r.name").alias("region"))
    .agg(F.sum(F.col("l.extendedprice") * (1 - F.col("l.discount"))).alias("revenue"))
)

# 2) Percentile rank within region
w = Window.partitionBy("regionkey").orderBy("revenue")

ranked = (
    cust_revenue
    .withColumn("percentile_rank", (F.percent_rank().over(w) * F.lit(100)))
    .withColumn("cume_percent",    (F.cume_dist().over(w)    * F.lit(100)))
    .select("custkey", "region", "revenue", "percentile_rank", "cume_percent")
    .orderBy("region", "revenue")
)

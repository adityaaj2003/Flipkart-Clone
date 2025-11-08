SELECT
    l_suppkey AS suppkey,
    o_custkey AS custkey,
    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
FROM
    lineitem l
JOIN
    orders o ON l.l_orderkey = o.o_orderkey
GROUP BY
    l_suppkey,
    o_custkey
ORDER BY
    total_revenue DESC
LIMIT 10;

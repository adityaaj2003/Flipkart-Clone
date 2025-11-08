SELECT
    p.p_partkey,
    p.p_name
FROM
    part p
JOIN
    lineitem l ON p.p_partkey = l.l_partkey
JOIN
    orders o ON l.l_orderkey = o.o_orderkey
JOIN
    customer c ON o.o_custkey = c.c_custkey
JOIN
    nation n ON c.c_nationkey = n.n_nationkey
GROUP BY
    p.p_partkey, p.p_name
HAVING
    COUNT(DISTINCT n.n_nationkey) >= 5
ORDER BY
    p.p_partkey;

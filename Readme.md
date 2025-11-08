WITH region_nation_avg AS (
    SELECT
        r.r_name,
        n.n_name,
        AVG(o.o_totalprice) AS avg_totalprice
    FROM region r
    JOIN nation n
      ON n.n_regionkey = r.r_regionkey
    JOIN customer c
      ON c.c_nationkey = n.n_nationkey
    JOIN orders o
      ON o.o_custkey = c.c_custkey
    GROUP BY r.r_name, n.n_name
),
ranked AS (
    SELECT
        r_name,
        n_name,
        avg_totalprice,
        ROW_NUMBER() OVER (
            PARTITION BY r_name
            ORDER BY avg_totalprice DESC
        ) AS rn
    FROM region_nation_avg
)
SELECT
    r_name,
    n_name,
    avg_totalprice
FROM ranked
WHERE rn = 1
ORDER BY r_name;

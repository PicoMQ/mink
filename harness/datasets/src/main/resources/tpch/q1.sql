SELECT l_returnflag, l_linestatus,
    sum(l_quantity), sum(l_extendedprice),
    sum(l_extendedprice * (1 - l_discount)),
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
    avg(l_quantity), avg(l_extendedprice), avg(l_discount),
    count(*)
FROM %s
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus

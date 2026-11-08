SELECT vehicle_id, count(*), min(located_at), max(located_at)
FROM %s
GROUP BY vehicle_id
ORDER BY vehicle_id

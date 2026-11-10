SELECT vehicle_id, lat, lon, heading, speed
FROM (
    SELECT vehicle_id, lat, lon, heading, speed,
        row_number() OVER (PARTITION BY vehicle_id ORDER BY located_at DESC) AS position
    FROM %s
) AS ranked
WHERE position = 1
ORDER BY vehicle_id

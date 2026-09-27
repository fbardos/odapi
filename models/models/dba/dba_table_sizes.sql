SELECT
    database,
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE active
GROUP BY
    database,
    table
ORDER BY sum(bytes_on_disk) DESC

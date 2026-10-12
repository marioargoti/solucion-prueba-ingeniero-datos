
-- Estadisticas globales independientes del acumulador incremental
SELECT
    COUNT(*) AS row_count,
    COUNT(price) AS price_count,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM transactions;


-- Estadisticas por archivo para comprobar la carga
SELECT
    source_file,
    COUNT(*) AS row_count,
    COUNT(price) AS price_count,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM transactions
GROUP BY source_file
ORDER BY source_file;


-- Estado mantenido incrementalmente
SELECT
    row_count,
    price_count,
    sum_price,
    CASE
        WHEN price_count > 0
        THEN sum_price / price_count
        ELSE NULL
    END AS avg_price,
    min_price,
    max_price
FROM pipeline_stats
WHERE id = 1;

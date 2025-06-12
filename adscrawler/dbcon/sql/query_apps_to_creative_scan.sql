WITH run_counts AS (
    SELECT
        saac.store_app,
        sa.store_id,
        count(*) AS api_calls
    FROM
        store_app_api_calls AS saac
    LEFT JOIN store_apps AS sa ON saac.store_app = sa.id
    WHERE saac.called_at <= current_date - INTERVAL '1 hour'
    GROUP BY
        store_app, sa.store_id
)

SELECT * FROM run_counts
WHERE api_calls > 0
ORDER BY api_calls DESC;

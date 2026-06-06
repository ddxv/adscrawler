WITH has_creatives AS (
    SELECT
        ac.run_id,
        sa.store_id
    FROM
        api_calls AS ac
    LEFT JOIN store_apps AS sa
        ON
            ac.store_app = sa.id
    WHERE
        (
            ac.request_mime_type
            ~* '(image|video)/(jpeg|jpg|png|gif|webp|webm|mp4|avi|quicktime)'
            OR ac.response_mime_type
            ~* '(image|video)/(jpeg|jpg|png|gif|webp|webm|mp4|avi|quicktime)'
        )
        AND ac.status_code = 200
        AND ac.response_size_bytes > 80000
        AND ac.called_at >= :earliest_date
        AND ac.called_at <= now() - INTERVAL '1 hour'
),
my_runs AS (
    SELECT DISTINCT
        run_id,
        store_id
    FROM
        has_creatives
),
last_runs AS (
    SELECT
        csr.run_id,
        max(csr.inserted_at) AS last_run_at
    FROM
        logging.creative_scan_results AS csr
    WHERE
        csr.run_id IN (
            SELECT mrr.run_id
            FROM
                my_runs AS mrr
        )
    GROUP BY
        csr.run_id
)
SELECT
    mr.run_id,
    mr.store_id,
    lr.last_run_at
FROM
    my_runs AS mr
LEFT JOIN last_runs AS lr
    ON
        mr.run_id = lr.run_id
ORDER BY
    lr.last_run_at ASC NULLS FIRST;

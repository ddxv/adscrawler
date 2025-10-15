WITH has_creatives AS (
    SELECT *
    FROM
        api_calls
    WHERE
        (
            request_mime_type
            ~* '(image|video)/(jpeg|jpg|png|gif|webp|webm|mp4|avi|quicktime)'
            OR
            response_mime_type
            ~* '(image|video)/(jpeg|jpg|png|gif|webp|webm|mp4|avi|quicktime)'
        ) AND status_code = 200
        AND response_size_bytes > 50000
),
run_counts AS (
    SELECT
        ac.store_app,
        ac.run_id,
        sa.store_id,
        count(*) AS api_calls
    FROM
        has_creatives AS ac
    LEFT JOIN store_apps AS sa ON ac.store_app = sa.id
    WHERE
        ac.called_at >= :earliest_date
        AND ac.called_at <= current_date - INTERVAL '1 hour'
    GROUP BY
        ac.store_app, ac.run_id, sa.store_id
)
SELECT * FROM run_counts
WHERE api_calls > 1;

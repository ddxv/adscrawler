WITH has_creatives AS (
    SELECT *
    FROM api_calls
    WHERE (
        request_mime_type  ~* '(image|video)/(jpeg|jpg|png|gif|webp|webm|mp4|avi|quicktime)'
        OR response_mime_type ~* '(image|video)/(jpeg|jpg|png|gif|webp|webm|mp4|avi|quicktime)'
    )
    AND status_code = 200
    AND response_size_bytes > 80000
),
all_api_calls AS (
    SELECT
        ac.store_app,
        ac.run_id,
        sa.store_id,
        ac.id AS api_call_id,
        ac.mitm_uuid
    FROM api_calls AS ac
    LEFT JOIN store_apps AS sa ON ac.store_app = sa.id
    WHERE ac.id IN (SELECT hc.id FROM has_creatives hc)
      AND ac.called_at >= :earliest_date
      AND ac.called_at <= current_date - INTERVAL '1 hour'
),
my_runs AS (
    SELECT DISTINCT run_id, store_id
    FROM all_api_calls
),
last_runs AS (
    SELECT run_id, max(inserted_at) AS last_run_at
    FROM logging.creative_scan_results
    WHERE run_id IN (SELECT run_id FROM my_runs)
    GROUP BY run_id
)
SELECT mr.run_id, mr.store_id, lr.last_run_at
FROM my_runs mr
LEFT JOIN last_runs lr ON mr.run_id = lr.run_id
ORDER BY last_run_at ASC NULLS FIRST;	

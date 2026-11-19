SELECT
    d.id,
    d.store,
    d.developer_id,
    d.name,
    app.store_id,
    COUNT(*) OVER () AS total_queue_depth
FROM public.developers AS d
JOIN LATERAL (
    SELECT sa.store_id
    FROM public.store_apps AS sa
    WHERE sa.developer = d.id
        AND sa.crawl_result = 1
    ORDER BY sa.updated_at DESC NULLS LAST, sa.id DESC
    LIMIT 1
) AS app ON TRUE
WHERE
    d.store = :store
    AND COALESCE(d.updated_at, d.created_at, 'epoch'::timestamp)
        <= NOW() - INTERVAL '6 months'
    AND d.id > :after_id
ORDER BY d.id
LIMIT :mylimit;

SELECT
    sa.id,
    sa.store_id,
    sa.store,
    sa.icon_url_512,
    sa.icon_128,
    sa.icon_64
FROM
    public.store_apps AS sa
LEFT JOIN logging.app_icons_crawled_at AS aica
    ON sa.id = aica.store_app
WHERE
    -- initial crawls
    -- (sa.icon_128 IS NULL AND sa.icon_64 IS NULL)
    (sa.icon_128 IS NULL)
    AND sa.icon_url_512 IS NOT NULL
    AND sa.crawl_result = 1
    AND (:store_filter IS NULL OR sa.store = :store_filter)
    AND (
        aica.store_app IS NULL
        -- initial crawls
        OR aica.crawled_at < CURRENT_DATE - INTERVAL '3 days'
    )
ORDER BY
    CASE
        WHEN sa.icon_128 IS NULL AND sa.icon_64 IS NULL THEN 0
        WHEN sa.icon_128 IS NULL OR sa.icon_64 IS NULL THEN 1
        ELSE 2
    END ASC,
    sa.updated_at DESC
LIMIT :mylimit;

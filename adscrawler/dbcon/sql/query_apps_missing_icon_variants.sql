SELECT
    sa.id,
    sa.store_id,
    sa.icon_url_512,
    sa.icon_128,
    sa.icon_64
FROM
    public.store_apps AS sa
    LEFT JOIN logging.app_icons_crawled_at AS aica
        ON sa.id = aica.store_app
WHERE
    (sa.icon_128 IS NULL OR sa.icon_64 IS NULL)
    AND sa.icon_url_512 IS NOT NULL
    AND sa.crawl_result = 1
    AND (aica.store_app IS NULL OR aica.crawled_at < CURRENT_DATE - INTERVAL '90 days')
ORDER BY
    sa.updated_at DESC NULLS LAST
LIMIT :mylimit;

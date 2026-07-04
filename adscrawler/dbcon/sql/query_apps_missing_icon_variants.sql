SELECT
    sa.id,
    sa.store_id,
    sa.icon_url_512,
    sa.icon_128,
    sa.icon_64
FROM
    public.store_apps AS sa
WHERE
    (sa.icon_128 IS NULL OR sa.icon_64 IS NULL)
    AND sa.icon_url_512 IS NOT NULL
    AND sa.crawl_result = 1
ORDER BY
    sa.updated_at DESC NULLS LAST
LIMIT :mylimit;

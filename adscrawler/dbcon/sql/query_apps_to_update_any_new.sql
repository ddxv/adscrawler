SELECT
    sa.store,
    sa.id AS store_app,
    sa.store_id,
    'US' AS country_code,
    sa.icon_url_100,
    sa.additional_html_scraped_at,
    sa.updated_at AS app_updated_at
FROM
    public.store_apps AS sa
WHERE
    sa.store = :store
    -- Always crawl new apps
    AND sa.crawl_result IS NULL
LIMIT :mylimit;

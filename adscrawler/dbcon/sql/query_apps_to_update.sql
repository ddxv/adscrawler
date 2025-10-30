WITH countries_to_crawl AS (
    SELECT
        cc.country_id,
        c.alpha2,
        cc.priority
    FROM
        public.crawl_scenario_country_config AS cc
    INNER JOIN public.countries AS c
        ON
            cc.country_id = c.id
    INNER JOIN public.crawl_scenarios AS s
        ON
            cc.scenario_id = s.id
    WHERE
        s.name = 'app_details'
        AND cc.enabled = TRUE
        AND cc.priority = :country_crawl_priority
),
latest_crawls AS (
    SELECT DISTINCT ON
    (
        store_app,
        country_id
    )
        store_app,
        country_id,
        crawled_at,
        crawl_result
    FROM
        logging.store_app_country_crawl
    ORDER BY
        store_app ASC,
        country_id ASC,
        crawled_at DESC
)
SELECT
    sa.store,
    sa.id AS store_app,
    sa.store_id,
    ctc.country_id,
    ctc.alpha2 AS country_code,
    ctc.priority,
    sa.icon_url_100,
    sa.additional_html_scraped_at,
    sa.updated_at AS app_updated_at,
    lc.crawled_at AS country_crawled_at
FROM
    public.store_apps AS sa
CROSS JOIN countries_to_crawl AS ctc
LEFT JOIN latest_crawls AS lc
    ON
        sa.id = lc.store_app
        AND ctc.country_id = lc.country_id
WHERE
    sa.store = :store
    AND (
        lc.crawled_at IS NULL OR (
            (
                (
                    installs >= :short_update_installs
                    OR rating_count >= :short_update_ratings
                )
                AND sa.updated_at <= :short_update_ts
                AND (
                    lc.crawl_result = 1
                    OR lc.crawl_result IS NULL
                    OR sa.created_at >= :long_update_ts
                    OR sa.store_last_updated >= :year_ago_ts
                )
            )
            OR (
                sa.updated_at <= :long_update_ts
                AND (
                    lc.crawl_result = 1
                    OR lc.crawl_result IS NULL
                    OR sa.store_last_updated >= :year_ago_ts
                )
            )
            OR (
                sa.updated_at <= :max_recrawl_ts
                OR lc.crawl_result IS NULL
            )
        )
    )
ORDER BY
    (CASE
        WHEN sa.crawl_result IS NULL
            THEN 0
        ELSE 1
    END),
    (CASE
        WHEN sa.updated_at < :max_recrawl_ts
            THEN 0
        ELSE 1
    END),
    ctc.priority,
    GREATEST(
        COALESCE(installs, 0),
        COALESCE(CAST(rating_count AS bigint), 0) * 50
    )
    DESC NULLS LAST
LIMIT :mylimit;

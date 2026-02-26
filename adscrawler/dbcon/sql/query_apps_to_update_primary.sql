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
        logging.app_country_crawls
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
    sa.additional_html_scraped_at AS html_last_scraped_at,
    sa.updated_at AS app_updated_at,
    lc.crawled_at AS country_crawled_at
FROM
    public.store_apps AS sa
CROSS JOIN countries_to_crawl AS ctc
LEFT JOIN app_global_metrics_latest AS agm ON sa.id = agm.store_app
LEFT JOIN latest_crawls AS lc
    ON
        sa.id = lc.store_app
        AND ctc.country_id = lc.country_id
WHERE
    sa.store = :store
    AND (
        -- Always crawl new apps
        sa.crawl_result IS NULL
        -- Short update conditions
        OR (
            (
                agm.total_installs >= :short_update_installs
                OR agm.rating_count >= :short_update_ratings
                OR (
                    sa.id IN (
                        SELECT sailr.store_app
                        FROM store_apps_in_latest_rankings AS sailr
                    )
                )
            )
            AND sa.updated_at <= :short_update_ts
            AND (
                sa.crawl_result = 1
                OR sa.crawl_result IS NULL
                OR sa.created_at >= :long_update_ts
                OR sa.store_last_updated >= :year_ago_ts
            )
        )
        -- Long update conditions
        OR (
            sa.updated_at <= :long_update_ts
            AND (
                sa.crawl_result = 1
                OR sa.crawl_result IS NULL
                OR sa.store_last_updated >= :year_ago_ts
            )
        )
        -- Max update conditions
        OR (
            sa.updated_at <= :max_recrawl_ts
        )
    )
ORDER BY
    (CASE WHEN sa.crawl_result IS NULL THEN 1 ELSE 0 END) DESC, -- always crawl new ones first
    (
        GREATEST(
            COALESCE(agm.total_installs, 0),
            COALESCE(agm.rating_count::bigint, 0)
        )
        * (10 * EXTRACT(DAY FROM (NOW() - sa.updated_at)))
    ) DESC
LIMIT :mylimit;

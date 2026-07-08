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
        -- Just returns one country US
        AND cc.priority = 1
),
ranked_apps AS (
    SELECT store_app
    FROM
        store_apps_in_latest_rankings
),
target_apps AS (
    SELECT
        sa.id,
        sa.store,
        sa.store_id,
        sa.store_last_updated,
        sa.crawl_result,
        sa.updated_at,
        sa.created_at,
        sa.release_date,
        sa.icon_url_100,
        sa.additional_html_scraped_at,
        CASE
            WHEN sa.crawl_result IS NULL THEN 1
            ELSE 0
        END
            AS crawl_result_null,
        CASE
            WHEN ra.store_app IS NOT NULL THEN 1
            ELSE 0
        END AS ranked_app,
        -- carry agm values in so we don't need to join it again later
        COALESCE(agm.total_installs, 0) AS total_installs,
        COALESCE(agm.total_ratings, 0) AS total_ratings
    FROM
        store_apps AS sa
    LEFT JOIN ranked_apps AS ra
        ON
            sa.id = ra.store_app
    LEFT JOIN app_global_metrics_latest AS agm
        ON
            sa.id = agm.store_app
    WHERE
        sa.store = :store
),
latest_crawls AS (
    SELECT DISTINCT ON
    (
        acc.store_app,
        acc.country_id
    )
        acc.store_app,
        acc.country_id,
        acc.crawled_at
    FROM
        logging.app_country_crawls AS acc
    INNER JOIN target_apps AS sa
        ON
            acc.store_app = sa.id
    WHERE
        country_id IN (
            SELECT country_id
            FROM
                countries_to_crawl
        )
    ORDER BY
        store_app ASC,
        country_id ASC,
        crawled_at DESC
)
SELECT
    ta.store,
    ta.id AS store_app,
    ta.store_id,
    ctc.country_id,
    ctc.alpha2 AS country_code,
    ctc.priority,
    ta.icon_url_100,
    COALESCE(ta.additional_html_scraped_at >= :year_ago_ts, FALSE)
        AS html_recently_scraped,
    ta.updated_at AS app_updated_at,
    lc.crawled_at AS country_crawled_at
FROM
    target_apps AS ta
CROSS JOIN countries_to_crawl AS ctc
LEFT JOIN latest_crawls AS lc
    ON
        ta.id = lc.store_app
        AND ctc.country_id = lc.country_id
WHERE
    (
        lc.crawled_at IS NULL
        -- prevent double crawling while waiting for daily S3 files to process
        OR lc.crawled_at < :short_update_ts
    )
    AND (
        ta.crawl_result IS NULL
        OR (
            (
                ta.total_installs >= :short_update_installs
                OR ta.total_ratings >= :short_update_ratings
                OR ta.ranked_app = 1
                OR ta.release_date > NOW() - INTERVAL '30 days'
            )
            AND lc.crawled_at <= :short_update_ts
            AND (
                ta.crawl_result = 1
                OR ta.crawl_result IS NULL
                OR ta.created_at >= :long_update_ts
                OR ta.store_last_updated >= :year_ago_ts
            )
        )
        OR (
            lc.crawled_at <= :long_update_ts
            AND (
                ta.crawl_result = 1
                OR ta.crawl_result IS NULL
                OR ta.store_last_updated >= :year_ago_ts
            )
        )
        OR lc.crawled_at <= :max_recrawl_ts
        OR lc.crawled_at IS NULL
    )
ORDER BY
    -- always crawl new ones first
    crawl_result_null DESC,
    --always crawl apps in rankings, sometimes new apps get missed
    ranked_app DESC,
    (
        GREATEST(total_installs, total_ratings)
        * 100 * EXTRACT(DAY FROM (NOW() - lc.crawled_at))
    ) DESC
LIMIT :mylimit;

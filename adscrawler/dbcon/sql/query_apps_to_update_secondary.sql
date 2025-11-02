WITH target_apps AS (
    SELECT
        sa.store,
        sa.id AS store_app,
        sa.store_id,
        sa.icon_url_100,
        sa.additional_html_scraped_at,
        sa.updated_at,
        sa.store_last_updated,
        agm.installs,
        agm.rating_count
    FROM
        store_apps AS sa
    LEFT JOIN app_global_metrics_latest AS agm
        ON
            sa.id = agm.store_app
    WHERE
        sa.store = :store
        AND (
            sa.crawl_result = 1
            OR sa.id IN (
                SELECT sailr.store_app
                FROM
                    store_apps_in_latest_rankings AS sailr
            )
        )
),
all_countries_to_crawl AS (
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
oldest_country_crawls AS (
    SELECT
        country_id,
        MIN(crawled_at) AS crawled_at
    FROM
        logging.app_country_crawls
    GROUP BY
        country_id
),
countries_to_crawl AS (
    SELECT
        actc.country_id,
        actc.alpha2,
        actc.priority
    FROM
        all_countries_to_crawl AS actc
    LEFT JOIN oldest_country_crawls AS occ
        ON
            actc.country_id = occ.country_id
    ORDER BY
        occ.crawled_at NULLS FIRST
    LIMIT 3
),
latest_app_crawls AS (
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
    WHERE
        country_id IN (
            SELECT cctc.country_id
            FROM
                countries_to_crawl AS cctc
        )
        AND store_app IN (
            SELECT tta.store_app
            FROM
                target_apps AS tta
        )
    ORDER BY
        store_app ASC,
        country_id ASC,
        crawled_at DESC
)
SELECT
    sa.store,
    sa.store_app,
    sa.store_id,
    ctc.country_id,
    ctc.alpha2 AS country_code,
    ctc.priority,
    sa.icon_url_100,
    sa.additional_html_scraped_at,
    sa.updated_at AS app_updated_at,
    lc.crawled_at AS country_crawled_at
FROM
    target_apps AS sa
CROSS JOIN countries_to_crawl AS ctc
LEFT JOIN latest_app_crawls AS lc
    ON
        sa.store_app = lc.store_app
        AND ctc.country_id = lc.country_id
WHERE
    -- Long update conditions
    (
        lc.crawled_at <= :long_update_ts
        AND
        sa.store_last_updated >= :year_ago_ts
    )
    -- Crawl at least once a year conditions
    OR (
        (
            lc.crawled_at <= :max_recrawl_ts
            OR lc.crawl_result IS NULL
        )
    )
ORDER BY
    (
        CASE
            WHEN lc.crawl_result IS NULL
                THEN 0
            ELSE 1
        END
    ),
    (
        CASE
            WHEN lc.crawled_at < :max_recrawl_ts
                THEN 0
            ELSE 1
        END
    ),
    GREATEST(
        COALESCE(sa.installs, 0),
        COALESCE(CAST(sa.rating_count AS bigint), 0)
    )
    DESC NULLS LAST
LIMIT :mylimit;

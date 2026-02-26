WITH target_apps AS (
    SELECT
        sa.store,
        sa.id AS store_app,
        sa.store_id,
        sa.icon_url_100,
        sa.updated_at,
        sa.store_last_updated,
        agm.total_installs AS installs,
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
            OR sa.store_last_updated > :year_ago_ts
        )
),
mycountries AS (
    SELECT DISTINCT
        c.alpha2 AS country_code,
        country_id
    FROM
        crawl_scenario_country_config AS cc
    LEFT JOIN countries AS c
        ON
            cc.country_id = c.id
    WHERE
        priority = :country_crawl_priority AND cc.scenario_id = 1
),
apps_last_crawled_at AS (
    SELECT DISTINCT ON
    (store_app)
        acmh.store_app,
        acmh.crawled_at,
        acmh.crawl_result
    FROM
        logging.app_country_crawls AS acmh
    INNER JOIN mycountries AS mc
        ON
            acmh.country_id = mc.country_id
    WHERE
        acmh.crawled_at > :max_recrawl_ts
    ORDER BY
        store_app ASC,
        crawled_at DESC
),
apps_to_crawl AS (
    SELECT
        sa.store,
        sa.store_app,
        sa.store_id,
        sa.icon_url_100,
        sa.updated_at AS app_updated_at,
        lc.crawled_at AS last_crawled_at
    FROM
        target_apps AS sa
    LEFT JOIN apps_last_crawled_at AS lc
        ON
            sa.store_app = lc.store_app
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
    LIMIT :mylimit
)
SELECT
    store,
    store_app,
    store_id,
    icon_url_100,
    app_updated_at,
    last_crawled_at,
    c.country_code
FROM
    apps_to_crawl
CROSS JOIN mycountries AS c;

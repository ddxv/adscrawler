CREATE MATERIALIZED VIEW public.mv_app_categories
TABLESPACE pg_default
AS
SELECT
    sa.store,
    cm.mapped_category AS category,
    count(*) AS app_count
FROM store_apps AS sa
INNER JOIN
    category_mapping AS cm
    ON sa.category::text = cm.original_category::text
WHERE sa.crawl_result = 1 AND sa.category IS NOT NULL
GROUP BY sa.store, cm.mapped_category
ORDER BY sa.store, cm.mapped_category;

--DROP INDEX IF EXISTS idx_mv_app_categories;
CREATE UNIQUE INDEX idx_mv_app_categories
ON mv_app_categories (store, category);

--DROP MATERIALIZED VIEW apps_new_weekly;
CREATE MATERIALIZED VIEW apps_new_weekly AS
WITH rankedapps AS (
    SELECT
        *,-- noqa: RF02
        row_number() OVER (
            PARTITION BY
                sa.store,
                cm.mapped_category
            ORDER BY
                sa.installs DESC NULLS LAST,
                sa.rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.release_date >= current_date - interval '7 days'
        AND sa.crawl_result = 1
)

SELECT *
FROM
    rankedapps
WHERE
    rn <= 100
WITH DATA;
--REFRESH MATERIALIZED VIEW apps_new_weekly ;

--DROP INDEX IF EXISTS idx_apps_new_weekly;
CREATE UNIQUE INDEX idx_apps_new_weekly
ON apps_new_weekly (store, mapped_category, store_id);


--DROP MATERIALIZED VIEW IF EXISTS apps_new_monthly;
CREATE MATERIALIZED VIEW apps_new_monthly AS
WITH rankedapps AS (
    SELECT
        *,-- noqa: RF02
        row_number() OVER (
            PARTITION BY
                sa.store,
                cm.mapped_category
            ORDER BY
                sa.installs DESC NULLS LAST,
                sa.rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.release_date >= current_date - interval '30 days'
        AND sa.crawl_result = 1
)

SELECT *
FROM
    rankedapps
WHERE
    rn <= 100
WITH DATA;
DROP INDEX IF EXISTS idx_apps_new_monthly;
CREATE UNIQUE INDEX idx_apps_new_monthly
ON apps_new_monthly (store, mapped_category, store_id);


--DROP MATERIALIZED VIEW IF EXISTS apps_new_yearly;
CREATE MATERIALIZED VIEW apps_new_yearly AS
WITH rankedapps AS (
    SELECT
        *,-- noqa: RF02
        row_number() OVER (
            PARTITION BY
                sa.store,
                cm.mapped_category
            ORDER BY
                sa.installs DESC NULLS LAST,
                sa.rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.release_date >= current_date - interval '365 days'
        AND sa.crawl_result = 1
)

SELECT *
FROM
    rankedapps
WHERE
    rn <= 100
WITH DATA;
DROP INDEX IF EXISTS idx_apps_new_yearly;
CREATE UNIQUE INDEX idx_apps_new_yearly
ON apps_new_yearly (store, mapped_category, store_id);



--DROP MATERIALIZED VIEW top_categories;
CREATE MATERIALIZED VIEW top_categories AS
WITH rankedapps AS (
    SELECT
        *, -- noqa: RF02
        row_number() OVER (
            PARTITION BY
                sa.store,
                cm.mapped_category
            ORDER BY
                sa.installs DESC NULLS LAST,
                sa.rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.crawl_result = 1
)

SELECT * FROM
    rankedapps
WHERE
    rn <= 50
WITH DATA;

--REFRESH MATERIALIZED VIEW top_categories ;

DROP INDEX IF EXISTS idx_top_categories;
CREATE UNIQUE INDEX idx_top_categories
ON top_categories (store, mapped_category, store_id);


CREATE MATERIALIZED VIEW store_apps_history_change AS
WITH date_diffs AS (
    SELECT
        store_app,
        country,
        crawled_date,
        installs,
        rating_count,
        max(crawled_date) OVER (
            PARTITION BY
                store_app,
                country
        ) AS last_date,
        installs - lead(installs) OVER (
            PARTITION BY
                store_app,
                country
            ORDER BY
                crawled_date DESC
        ) AS installs_diff,
        rating_count - lead(rating_count) OVER (
            PARTITION BY
                store_app,
                country
            ORDER BY
                crawled_date DESC
        ) AS rating_count_diff,
        crawled_date - lead(crawled_date) OVER (
            PARTITION BY
                store_app,
                country
            ORDER BY
                crawled_date DESC
        ) AS days_diff
    FROM
        store_apps_country_history
    WHERE
        installs > 1000
        OR rating_count > 20
),

weekly_averages AS (
    SELECT
        date_trunc(
            'week',
            crawled_date
        )::date AS week_start,
        store_app,
        country,
        sum(installs_diff) AS total_installs_diff,
        sum(rating_count) AS rating_count_diff,
        sum(days_diff) AS total_days
    FROM
        date_diffs
    GROUP BY
        date_trunc(
            'week',
            crawled_date
        )::date,
        store_app,
        country
)

SELECT
    week_start,
    store_app,
    country,
    round(
        total_installs_diff / nullif(
            total_days,
            0
        )
    ) AS avg_daily_installs_diff,
    round(
        rating_count_diff / nullif(
            total_days,
            0
        )
    ) AS rating_count_diff
FROM
    weekly_averages
ORDER BY
    week_start DESC,
    store_app ASC,
    country ASC
WITH DATA;

DROP INDEX IF EXISTS idx_store_apps_history_change;
CREATE UNIQUE INDEX idx_store_apps_history_change
ON store_apps_history_change (week_start, store_app, country);

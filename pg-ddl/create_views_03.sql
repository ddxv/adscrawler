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


CREATE MATERIALIZED VIEW public.store_apps_history_weekly
TABLESPACE pg_default
AS
WITH date_diffs AS (
    SELECT
        sach.store_app,
        sach.country_id,
        sach.crawled_date,
        sach.installs,
        sach.rating_count,
        max(sach.crawled_date) OVER (
            PARTITION BY
                sach.store_app,
                sach.country_id
        ) AS last_date,
        sach.installs - lead(sach.installs) OVER (
            PARTITION BY
                sach.store_app,
                sach.country_id
            ORDER BY
                sach.crawled_date DESC
        ) AS installs_diff,
        sach.rating_count - lead(sach.rating_count) OVER (
            PARTITION BY
                sach.store_app,
                sach.country_id
            ORDER BY
                sach.crawled_date DESC
        ) AS rating_count_diff,
        sach.crawled_date - lead(sach.crawled_date) OVER (
            PARTITION BY
                sach.store_app,
                sach.country_id
            ORDER BY
                sach.crawled_date DESC
        ) AS days_diff
    FROM
        store_apps_country_history AS sach
    WHERE
        (
            sach.store_app IN (
                SELECT sa.id
                FROM
                    store_apps AS sa
                WHERE
                    sa.crawl_result = 1
            )
            AND sach.crawled_date > current_date - interval '375 days'
        )
),

weekly_totals AS (
    SELECT
        date_trunc(
            'week'::text,
            date_diffs.crawled_date::timestamp with time zone
        )::date AS week_start,
        date_diffs.store_app,
        date_diffs.country_id,
        sum(date_diffs.installs_diff) AS installs_diff,
        sum(date_diffs.rating_count_diff) AS rating_count_diff,
        sum(date_diffs.days_diff) AS days_diff
    FROM
        date_diffs
    GROUP BY
        (
            date_trunc(
                'week'::text,
                date_diffs.crawled_date::timestamp with time zone
            )::date
        ),
        date_diffs.store_app,
        date_diffs.country_id
)

SELECT
    week_start,
    store_app,
    country_id,
    installs_diff,
    rating_count_diff
FROM
    weekly_totals
ORDER BY
    week_start DESC,
    store_app ASC,
    country_id ASC
WITH DATA;

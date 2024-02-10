CREATE MATERIALIZED VIEW mv_app_categories AS
SELECT
    sa.store,
    cm.mapped_category AS category,
    COUNT(*) AS app_count
FROM
    store_apps AS sa
INNER JOIN category_mapping AS cm
    ON
        sa.category = cm.original_category
GROUP BY
    sa.store,
    cm.mapped_category
ORDER BY
    sa.store,
    cm.mapped_category;

--DROP MATERIALIZED VIEW apps_new_weekly;
CREATE MATERIALIZED VIEW apps_new_weekly AS
WITH rankedapps AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                store,
                mapped_category
            ORDER BY
                installs DESC NULLS LAST,
                rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.release_date >= CURRENT_DATE - INTERVAL '7 days'
        AND crawl_result = 1
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
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                store,
                mapped_category
            ORDER BY
                installs DESC NULLS LAST,
                rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.release_date >= CURRENT_DATE - INTERVAL '30 days'
        AND crawl_result = 1
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


DROP MATERIALIZED VIEW IF EXISTS apps_new_yearly;
CREATE MATERIALIZED VIEW apps_new_yearly AS
WITH rankedapps AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                store,
                mapped_category
            ORDER BY
                installs DESC NULLS LAST,
                rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps AS sa
    INNER JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.release_date >= CURRENT_DATE - INTERVAL '365 days'
        AND crawl_result = 1
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
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                store,
                mapped_category
            ORDER BY
                installs DESC NULLS LAST,
                rating_count DESC NULLS LAST
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



CREATE MATERIALIZED VIEW top_trackers AS
WITH latest_version_codes AS (
    SELECT
        id,
        store_app,
        MAX(version_code) AS version_code
    FROM
        version_codes
    GROUP BY
        id,
        store_app
),

trackers_per_app AS (
    SELECT DISTINCT
        vc.store_app,
        vc.version_code,
        tm.tracker
    FROM
        version_details AS vd
    LEFT JOIN latest_version_codes AS vc
        ON
            vd.version_code = vc.id
    LEFT JOIN tracker_package_map AS tm ON
        vd.android_name ~* tm.package_pattern
),

tracker_counts AS (
    SELECT
        tracker,
        COUNT(DISTINCT store_app)
    FROM
        trackers_per_app
    GROUP BY
        tracker
)

SELECT
    t.id AS tracker,
    t.name AS tracker_name,
    tc.count
FROM
    tracker_counts AS tc
LEFT JOIN trackers AS t
    ON
        tc.tracker = t.id
WITH DATA;

DROP INDEX IF EXISTS idx_top_trackers;
CREATE UNIQUE INDEX idx_top_trackers
ON top_trackers (tracker);

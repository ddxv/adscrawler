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
        *,-- noqa: RF02
        ROW_NUMBER() OVER (
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
        sa.release_date >= CURRENT_DATE - INTERVAL '7 days'
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
        ROW_NUMBER() OVER (
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
        sa.release_date >= CURRENT_DATE - INTERVAL '30 days'
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


DROP MATERIALIZED VIEW IF EXISTS apps_new_yearly;
CREATE MATERIALIZED VIEW apps_new_yearly AS
WITH rankedapps AS (
    SELECT
        *,-- noqa: RF02
        ROW_NUMBER() OVER (
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
        sa.release_date >= CURRENT_DATE - INTERVAL '365 days'
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
        ROW_NUMBER() OVER (
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

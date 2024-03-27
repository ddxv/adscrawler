CREATE MATERIALIZED VIEW adtech.store_apps_companies AS
WITH latest_version_codes AS (
    SELECT
        id,
        store_app,
        MAX(version_code) AS version_code
    FROM
        public.version_codes
    WHERE
        crawl_result = 1
    GROUP BY
        id,
        store_app
),

apps_with_companies AS (
    SELECT DISTINCT
        vc.store_app,
        tm.company_id,
        COALESCE(
            pc.parent_company_id,
            tm.company_id
        ) AS parent_id
    FROM
        latest_version_codes AS vc
    LEFT JOIN public.version_details AS vd
        ON
            vc.id = vd.version_code
    INNER JOIN adtech.sdk_packages AS tm
        ON
            vd.android_name ILIKE tm.package_pattern || '%'
    LEFT JOIN adtech.companies AS pc ON
        tm.company_id = pc.id
)

SELECT
    awc.store_app,
    awc.company_id,
    awc.parent_id
FROM
    apps_with_companies AS awc
UNION ALL
SELECT
    vc.store_app,
    -- Note: 10 is tracker db id for no network found
    10 AS company_id,
    10 AS parent_id
FROM
    latest_version_codes AS vc
WHERE
    vc.store_app NOT IN (
        SELECT store_app
        FROM
            apps_with_companies
    )
WITH DATA;



DROP INDEX IF EXISTS idx_store_apps_companies;
CREATE UNIQUE INDEX idx_store_apps_companies
ON adtech.store_apps_companies (store_app, company_id);


CREATE MATERIALIZED VIEW adtech.companies_by_d30_counts AS
WITH cat_hist_totals AS (
    SELECT
        cm.mapped_category,
        SUM(sahc.avg_daily_installs_diff * 7) AS installs,
        SUM(sahc.rating_count_diff * 7) AS ratings
    FROM
        store_apps_history_change AS sahc
    LEFT JOIN store_apps AS sa
        ON
            sahc.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sahc.week_start >= CURRENT_DATE - INTERVAL '30 days'
        AND sahc.store_app IN (
            SELECT DISTINCT store_app
            FROM
                adtech.store_apps_companies
        )
    GROUP BY
        cm.mapped_category
),

cat_app_totals AS (
    SELECT
        cm.mapped_category,
        COUNT(DISTINCT sac.store_app) AS category_total_apps,
        SUM(sa.installs) AS alltime_installs
    FROM
        adtech.store_apps_companies AS sac
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    GROUP BY
        cm.mapped_category
),

company_installs AS (
    SELECT
        cm.mapped_category,
        sac.company_id,
        c.name,
        c.parent_company_id,
        COALESCE(
            pc.name,
            c.name
        ) AS parent_company_name,
        SUM(hist.avg_daily_installs_diff * 7) AS installs,
        SUM(hist.rating_count_diff * 7) AS ratings,
        COUNT(DISTINCT sac.store_app) AS app_count
    FROM
        store_apps_history_change AS hist
    INNER JOIN adtech.store_apps_companies AS sac
        ON
            hist.store_app = sac.store_app
    LEFT JOIN adtech.companies AS c
        ON
            sac.company_id = c.id
    LEFT JOIN adtech.companies AS pc
        ON
            c.parent_company_id = pc.id
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        hist.week_start >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        cm.mapped_category,
        sac.company_id,
        c.name,
        c.parent_company_id,
        pc.name
    ORDER BY
        installs DESC
)

SELECT
    ci.mapped_category,
    ci.company_id,
    ci.name AS company_name,
    ci.parent_company_name,
    ccat.category_id,
    cat.name AS category_name,
    ci.installs,
    ci.ratings,
    ci.app_count,
    ct.category_total_apps,
    t.installs AS total_installs,
    ci.app_count::FLOAT / ct.category_total_apps AS app_count_percent,
    ci.ratings / t.ratings AS total_ratings_percent,
    ci.installs / t.installs AS total_installs_percent
FROM
    company_installs AS ci
LEFT JOIN adtech.company_categories AS ccat
    ON
        ci.company_id = ccat.company_id
LEFT JOIN adtech.categories AS cat
    ON
        ccat.category_id = cat.id
LEFT JOIN cat_hist_totals AS t
    ON
        ci.mapped_category = t.mapped_category
LEFT JOIN cat_app_totals AS ct
    ON
        ci.mapped_category = ct.mapped_category
WITH DATA;


-- DROP INDEX IF EXISTS adtech.companies_d30_counts_idx;
CREATE UNIQUE INDEX companies_d30_counts_idx
ON
adtech.companies_by_d30_counts (company_name);

CREATE MATERIALIZED VIEW adtech.companies_parent_by_d30_counts AS
WITH cat_hist_totals AS (
    SELECT
        cm.mapped_category,
        SUM(sahc.avg_daily_installs_diff * 7) AS installs,
        SUM(sahc.rating_count_diff * 7) AS ratings
    FROM
        store_apps_history_change AS sahc
    LEFT JOIN store_apps AS sa
        ON
            sahc.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sahc.week_start >= CURRENT_DATE - INTERVAL '30 days'
        AND sahc.store_app IN (
            SELECT DISTINCT store_app
            FROM
                adtech.store_apps_companies
        )
    GROUP BY
        cm.mapped_category
),

cat_app_totals AS (
    SELECT
        cm.mapped_category,
        COUNT(DISTINCT sac.store_app) AS category_total_apps,
        SUM(sa.installs) AS alltime_installs
    FROM
        adtech.store_apps_companies AS sac
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    GROUP BY
        cm.mapped_category
),

store_apps_parent_companies AS (
    SELECT DISTINCT
        sac.store_app,
        sac.parent_id,
        cats.category_id
    FROM
        adtech.store_apps_companies AS sac
    LEFT JOIN adtech.company_categories AS cats
        ON
            sac.company_id = cats.company_id
),

company_installs AS (
    SELECT
        cm.mapped_category,
        sac.category_id,
        sac.parent_id,
        SUM(hist.avg_daily_installs_diff * 7) AS installs,
        SUM(hist.rating_count_diff * 7) AS ratings,
        COUNT(DISTINCT sac.store_app) AS app_count
    FROM
        store_apps_history_change AS hist
    INNER JOIN store_apps_parent_companies AS sac
        ON
            hist.store_app = sac.store_app
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        hist.week_start >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        cm.mapped_category,
        sac.parent_id,
        sac.category_id
    ORDER BY
        installs DESC
)

SELECT
    ci.mapped_category,
    ci.parent_id AS company_id,
    com.name AS company_name,
    ci.category_id,
    ci.installs,
    ci.ratings,
    ci.app_count,
    tc.category_total_apps,
    t.installs AS total_installs,
    ci.app_count::FLOAT / tc.category_total_apps AS app_count_percent,
    ci.ratings / t.ratings AS total_ratings_percent,
    ci.installs / t.installs AS total_installs_percent
FROM
    company_installs AS ci
LEFT JOIN adtech.companies AS com
    ON
        ci.parent_id = com.id
LEFT JOIN cat_hist_totals AS t
    ON
        ci.mapped_category = t.mapped_category
LEFT JOIN cat_app_totals AS tc
    ON
        ci.mapped_category = tc.mapped_category
WITH DATA;


-- DROP INDEX IF EXISTS adtech.companies_d30_counts_idx;
CREATE UNIQUE INDEX companies_parent_d30_counts_idx
ON
adtech.companies_parent_by_d30_counts (company_name);

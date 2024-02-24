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
        tm.company_id
    FROM
        latest_version_codes AS vc
    LEFT JOIN public.version_details AS vd
        ON
            vc.id = vd.version_code
    INNER JOIN adtech.sdk_packages AS tm ON
        vd.android_name ILIKE tm.package_pattern || '%'
)

SELECT
    awc.store_app,
    awc.company_id
FROM
    apps_with_companies AS awc
UNION ALL
SELECT
    vc.store_app,
    -- Note: 10 is tracker db id for no network found
    10 AS company_id
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
WITH totals AS (
    SELECT
        SUM(avg_daily_installs_diff * 7) AS installs,
        SUM(rating_count_diff * 7) AS ratings
    FROM
        store_apps_history_change
    WHERE
        week_start >= CURRENT_DATE - INTERVAL '30 days'
        AND store_app IN (
            SELECT DISTINCT store_app
            FROM
                adtech.store_apps_companies
        )
),

company_installs AS (
    SELECT
        sac.company_id,
        c.name,
        c.parent_company_id,
        pc.name AS parent_company_name,
        SUM(hist.avg_daily_installs_diff * 7) AS installs,
        SUM(hist.rating_count_diff * 7) AS ratings,
        (
            SELECT installs
            FROM
                totals
        ) AS total_installs,
        SUM(hist.rating_count_diff * 7) / (
            SELECT ratings
            FROM
                totals
        ) AS total_ratings_percent,
        SUM(hist.avg_daily_installs_diff * 7) / (
            SELECT installs
            FROM
                totals
        ) AS total_installs_percent
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
    WHERE
        hist.week_start >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        sac.company_id,
        c.name,
        c.parent_company_id,
        pc.name
    ORDER BY
        installs DESC
)

SELECT
    ci.company_id,
    ci.name AS company_name,
    ci.parent_company_name,
    ccat.category_id,
    cat.name AS category_name,
    ci.installs,
    ci.ratings,
    ci.total_installs,
    ci.total_installs_percent,
    ci.total_ratings_percent
FROM
    company_installs AS ci
LEFT JOIN adtech.company_categories AS ccat
    ON
        ci.company_id = ccat.company_id
LEFT JOIN adtech.categories AS cat
    ON
        ccat.category_id = cat.id
WITH DATA;


-- DROP INDEX IF EXISTS adtech.companies_d30_counts_idx;
CREATE UNIQUE INDEX companies_d30_counts_idx
ON
adtech.companies_by_d30_counts (company_name);


CREATE MATERIALIZED VIEW companies_parent_by_d30_counts AS
WITH totals AS (
    SELECT
        SUM(avg_daily_installs_diff * 7) AS installs,
        SUM(rating_count_diff * 7) AS ratings
    FROM
        store_apps_history_change
    WHERE
        week_start >= CURRENT_DATE - INTERVAL '30 days'
        AND store_app IN (
            SELECT DISTINCT store_app
            FROM
                adtech.store_apps_companies
        )
),

company_installs AS (
    SELECT
        COALESCE(
            pc.parent_company_id,
            c.id
        ) AS self_or_parent_id,
        SUM(hist.avg_daily_installs_diff * 7) AS installs,
        SUM(hist.rating_count_diff * 7) AS ratings,
        (
            SELECT installs
            FROM
                totals
        ) AS total_installs,
        SUM(hist.rating_count_diff * 7) / (
            SELECT ratings
            FROM
                totals
        ) AS total_ratings_percent,
        SUM(hist.avg_daily_installs_diff * 7) / (
            SELECT installs
            FROM
                totals
        ) AS total_installs_percent
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
    WHERE
        hist.week_start >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        self_or_parent_id
    ORDER BY
        installs DESC
)

SELECT
    ci.self_or_parent_id AS company_id,
    com.name AS company_name,
    ccat.category_id,
    cat.name AS category_name,
    ci.installs,
    ci.ratings,
    ci.total_installs,
    ci.total_installs_percent,
    ci.total_ratings_percent
FROM
    company_installs AS ci
LEFT JOIN adtech.company_categories AS ccat
    ON
        ci.self_or_parent_id = ccat.company_id
LEFT JOIN adtech.categories AS cat
    ON
        ccat.category_id = cat.id
LEFT JOIN adtech.companies AS com
    ON
        ci.self_or_parent_id = com.id
WITH DATA;

-- DROP INDEX IF EXISTS adtech.companies_d30_counts_idx;
CREATE UNIQUE INDEX companies_parent_d30_counts_idx
ON
adtech.companies_parent_by_d30_counts (company_name);

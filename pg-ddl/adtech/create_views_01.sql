DROP MATERIALIZED VIEW IF EXISTS adtech.store_apps_companies_sdk CASCADE;
CREATE MATERIALIZED VIEW adtech.store_apps_companies_sdk AS
WITH latest_version_codes AS (
    SELECT DISTINCT ON
    (version_codes.store_app)
        -- Ensures one row per store_app WHEN combined WITH ORDER BY 
        version_codes.id,
        version_codes.store_app,
        version_codes.version_code
    FROM
        version_codes
    WHERE
        version_codes.crawl_result = 1
    ORDER BY
        version_codes.store_app,
        string_to_array(
            version_codes.version_code,
            '.'
        )::bigint [] DESC
),

sdk_apps_with_companies AS (
    SELECT DISTINCT
        vc.store_app,
        vnpm.company_id,
        coalesce(
            pc.parent_company_id,
            vnpm.company_id
        ) AS parent_id
    FROM
        latest_version_codes AS vc
    LEFT JOIN version_details AS vd ON vc.id = vd.version_code
    INNER JOIN
        adtech.company_value_name_package_mapping AS vnpm
        ON vd.value_name = vnpm.value_name
    LEFT JOIN adtech.companies AS pc ON
        vnpm.company_id = pc.id
),

sdk_paths_with_companies AS (
    SELECT DISTINCT
        vc.store_app,
        sd.company_id,
        coalesce(
            pc.parent_company_id,
            sd.company_id
        ) AS parent_id
    FROM
        latest_version_codes AS vc
    LEFT JOIN version_details AS vd
        ON
            vc.id = vd.version_code
    INNER JOIN adtech.sdk_paths AS ptm
        ON
            vd.value_name ~~* (
                ptm.path_pattern::text || '%'::text
            )
    LEFT JOIN adtech.sdks AS sd
        ON
            ptm.sdk_id = sd.id
    LEFT JOIN adtech.companies AS pc ON
        sd.company_id = pc.id
),

dev_apps_with_companies AS (
    SELECT DISTINCT
        sa.id AS store_app,
        cd.company_id,
        coalesce(
            pc.parent_company_id,
            cd.company_id
        ) AS parent_id
    FROM
        adtech.company_developers AS cd
    LEFT JOIN store_apps AS sa
        ON
            cd.developer_id = sa.developer
    LEFT JOIN adtech.companies AS pc ON
        cd.company_id = pc.id
),

all_apps_with_companies AS (
    SELECT
        sawc.store_app,
        sawc.company_id,
        sawc.parent_id
    FROM
        sdk_apps_with_companies AS sawc
    UNION
    SELECT
        spwc.store_app,
        spwc.company_id,
        spwc.parent_id
    FROM
        sdk_paths_with_companies AS spwc
    UNION
    SELECT
        dawc.store_app,
        dawc.company_id,
        dawc.parent_id
    FROM
        dev_apps_with_companies AS dawc
),

distinct_apps_with_cats AS (
    SELECT DISTINCT
        aawc.store_app,
        c.id AS category_id
    FROM
        all_apps_with_companies AS aawc
    LEFT JOIN adtech.company_categories AS cc
        ON
            aawc.company_id = cc.company_id
    LEFT JOIN adtech.categories AS c ON
        cc.category_id = c.id
),

distinct_store_apps AS (
    SELECT DISTINCT lvc.store_app
    FROM
        latest_version_codes AS lvc
),

all_combinations AS (
    SELECT
        sa.store_app,
        c.id AS category_id
    FROM
        distinct_store_apps AS sa
    CROSS JOIN adtech.categories AS c
),

unmatched_apps AS (
    SELECT DISTINCT
        ac.store_app,
        -ac.category_id AS company_id,
        -- NOTE: the negative category id IS SET AS a special company
        -ac.category_id AS parent_id
        -- NOTE: the negative category id IS SET AS a special company
    FROM
        all_combinations AS ac
    LEFT JOIN distinct_apps_with_cats AS dawc
        ON
            ac.store_app = dawc.store_app
            AND ac.category_id = dawc.category_id
    WHERE
        dawc.store_app IS NULL
        --ONLY unmatched apps
),

final_union AS (
    SELECT
        aawc.store_app,
        aawc.company_id,
        aawc.parent_id
    FROM
        all_apps_with_companies AS aawc
    UNION
    SELECT
        ua.store_app,
        ua.company_id,
        ua.parent_id
    FROM
        unmatched_apps AS ua
)

SELECT
    store_app,
    company_id,
    parent_id
FROM
    final_union
WITH DATA;


DROP INDEX IF EXISTS idx_store_apps_companies;
CREATE UNIQUE INDEX idx_store_apps_companies_sdk
ON adtech.store_apps_companies_sdk (store_app, company_id, parent_id);


CREATE MATERIALIZED VIEW adtech.companies_by_d30_counts AS
WITH cat_hist_totals AS (
    SELECT
        sa.store,
        cm.mapped_category,
        sum(sahc.avg_daily_installs_diff * 7) AS installs,
        sum(sahc.rating_count_diff * 7) AS ratings
    FROM
        store_apps_history_change AS sahc
    LEFT JOIN store_apps AS sa
        ON
            sahc.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sahc.week_start >= current_date - interval '30 days'
        AND sahc.store_app IN (
            SELECT DISTINCT sac.store_app
            FROM
                adtech.store_apps_companies_sdk AS sac
        )
    GROUP BY
        sa.store,
        cm.mapped_category
),

cat_app_totals AS (
    SELECT
        sa.store,
        cm.mapped_category,
        count(DISTINCT sac.store_app) AS category_total_apps,
        sum(sa.installs) AS alltime_installs
    FROM
        adtech.store_apps_companies_sdk AS sac
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    GROUP BY
        sa.store,
        cm.mapped_category
),

company_installs AS (
    SELECT
        sa.store,
        cm.mapped_category,
        sac.company_id,
        c.name,
        c.parent_company_id,
        coalesce(
            pc.name,
            c.name
        ) AS parent_company_name,
        sum(hist.avg_daily_installs_diff * 7) AS installs,
        sum(hist.rating_count_diff * 7) AS ratings,
        count(DISTINCT sac.store_app) AS app_count
    FROM
        adtech.store_apps_companies_sdk AS sac
    LEFT JOIN
        store_apps_history_change AS hist
        ON
            sac.store_app = hist.store_app
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
        hist.week_start >= current_date - interval '30 days'
        OR hist.week_start IS NULL
    GROUP BY
        sa.store,
        cm.mapped_category,
        sac.company_id,
        c.name,
        c.parent_company_id,
        pc.name
    ORDER BY
        installs DESC
)

SELECT
    ci.store,
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
    t.ratings AS total_ratings,
    ci.app_count::float / ct.category_total_apps AS app_count_percent,
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
        ci.store = t.store
        AND
        ci.mapped_category = t.mapped_category
LEFT JOIN cat_app_totals AS ct
    ON
        ci.store = ct.store
        AND
        ci.mapped_category = ct.mapped_category
WHERE
    ci.installs IS NOT NULL
    OR ci.ratings IS NOT NULL
WITH DATA;


-- DROP INDEX IF EXISTS adtech.companies_d30_counts_idx;
CREATE UNIQUE INDEX companies_d30_counts_idx
ON
adtech.companies_by_d30_counts (
    store, mapped_category, category_id, company_name
);


CREATE MATERIALIZED VIEW adtech.companies_parent_by_d30_counts AS
WITH cat_hist_totals AS (
    SELECT
        sa.store,
        cm.mapped_category,
        sum(sahc.avg_daily_installs_diff * 7) AS installs,
        sum(sahc.rating_count_diff * 7) AS ratings
    FROM
        store_apps_history_change AS sahc
    LEFT JOIN store_apps AS sa
        ON
            sahc.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sahc.week_start >= current_date - interval '30 days'
        AND sahc.store_app IN (
            SELECT DISTINCT sac.store_app
            FROM
                adtech.store_apps_companies_sdk AS sac
        )
    GROUP BY
        sa.store,
        cm.mapped_category
),

cat_app_totals AS (
    SELECT
        sa.store,
        cm.mapped_category,
        count(DISTINCT sac.store_app) AS category_total_apps,
        sum(sa.installs) AS alltime_installs
    FROM
        adtech.store_apps_companies_sdk AS sac
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    GROUP BY
        sa.store,
        cm.mapped_category
),

store_apps_parent_companies AS (
    SELECT DISTINCT
        sac.store_app,
        sac.parent_id,
        cats.category_id
    FROM
        adtech.store_apps_companies_sdk AS sac
    LEFT JOIN adtech.company_categories AS cats
        ON
            sac.company_id = cats.company_id
),

company_installs AS (
    SELECT
        sa.store,
        cm.mapped_category,
        sac.category_id,
        sac.parent_id,
        sum(hist.avg_daily_installs_diff * 7) AS installs,
        sum(hist.rating_count_diff * 7) AS ratings,
        count(DISTINCT sac.store_app) AS app_count
    FROM
        store_apps_parent_companies AS sac
    LEFT JOIN
        store_apps_history_change AS hist
        ON
            sac.store_app = hist.store_app
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        hist.week_start >= current_date - interval '30 days'
        OR hist.week_start IS NULL
    GROUP BY
        sa.store,
        cm.mapped_category,
        sac.parent_id,
        sac.category_id
    ORDER BY
        installs DESC
)

SELECT
    ci.store,
    ci.mapped_category,
    ci.parent_id AS company_id,
    com.name AS company_name,
    ci.category_id,
    ci.installs,
    ci.ratings,
    ci.app_count,
    tc.category_total_apps,
    t.installs AS total_installs,
    t.ratings AS total_ratings,
    ci.app_count::float / tc.category_total_apps AS app_count_percent,
    ci.ratings / t.ratings AS total_ratings_percent,
    ci.installs / t.installs AS total_installs_percent
FROM
    company_installs AS ci
LEFT JOIN adtech.companies AS com
    ON
        ci.parent_id = com.id
LEFT JOIN cat_hist_totals AS t
    ON
        ci.store = t.store
        AND
        ci.mapped_category = t.mapped_category
LEFT JOIN cat_app_totals AS tc
    ON
        ci.store = tc.store
        AND
        ci.mapped_category = tc.mapped_category
WHERE
    ci.installs IS NOT NULL OR ci.ratings IS NOT NULL
WITH DATA;


CREATE UNIQUE INDEX companies_parent_d30_counts_idx
ON
adtech.companies_parent_by_d30_counts (
    store, mapped_category, category_id, company_name
);



CREATE MATERIALIZED VIEW adtech.combined_store_apps_parent_companies
TABLESPACE pg_default
AS
SELECT DISTINCT
    csac.store_app,
    csac.app_category,
    csac.parent_id AS company_id,
    csac.tag_source,
    coalesce(
        ad.domain,
        csac.ad_domain
    ) AS ad_domain
FROM
    adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.companies AS c
    ON
        csac.parent_id = c.id
LEFT JOIN adtech.company_domain_mapping AS cdm
    ON
        c.id = cdm.company_id
LEFT JOIN ad_domains AS ad ON
    cdm.domain_id = ad.id;




CREATE MATERIALIZED VIEW adtech.company_value_name_package_mapping
TABLESPACE pg_default
AS SELECT DISTINCT
    vd.value_name,
    sd.company_id,
    sp.package_pattern
FROM version_details AS vd
INNER JOIN
    adtech.sdk_packages AS sp
    ON vd.value_name ~~* (sp.package_pattern::text || '%'::text)
LEFT JOIN adtech.sdks AS sd
    ON sp.sdk_id = sd.id
WITH DATA;

-- View indexes:
CREATE INDEX company_value_name_package_mapping_company_id_idx ON adtech.company_value_name_package_mapping USING btree (
    company_id
);
CREATE INDEX company_value_name_package_mapping_value_name_idx ON adtech.company_value_name_package_mapping USING btree (
    value_name
);

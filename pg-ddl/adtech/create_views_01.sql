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

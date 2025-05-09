CREATE MATERIALIZED VIEW adtech.store_apps_companies_sdk
TABLESPACE pg_default
AS
WITH latest_version_codes AS (
    SELECT DISTINCT ON (version_codes.store_app)
        version_codes.id,
        version_codes.store_app,
        version_codes.version_code
    FROM version_codes
    WHERE version_codes.crawl_result = 1
    ORDER BY
        version_codes.store_app,
        (
            string_to_array(
                version_codes.version_code::text, '.'::text
            )::bigint []
        ) DESC
),

sdk_apps_with_companies AS (
    SELECT DISTINCT
        vc.store_app,
        cvsm.company_id,
        coalesce(pc.parent_company_id, cvsm.company_id) AS parent_id
    FROM latest_version_codes AS vc
    LEFT JOIN version_details_map AS vdm ON vc.id = vdm.version_code
    INNER JOIN
        adtech.company_value_string_mapping AS cvsm
        ON vdm.string_id = cvsm.version_string_id
    LEFT JOIN adtech.companies AS pc ON cvsm.company_id = pc.id
),

sdk_paths_with_companies AS (
    SELECT DISTINCT
        vc.store_app,
        sd.company_id,
        coalesce(pc.parent_company_id, sd.company_id) AS parent_id
    FROM latest_version_codes AS vc
    LEFT JOIN version_details_map AS vdm ON vc.id = vdm.version_code
    LEFT JOIN version_strings AS vs ON vdm.string_id = vs.id
    INNER JOIN
        adtech.sdk_paths AS ptm
        ON vs.value_name ~~* (ptm.path_pattern::text || '%'::text)
    LEFT JOIN adtech.sdks AS sd ON ptm.sdk_id = sd.id
    LEFT JOIN adtech.companies AS pc ON sd.company_id = pc.id
),

dev_apps_with_companies AS (
    SELECT DISTINCT
        sa.id AS store_app,
        cd.company_id,
        coalesce(pc.parent_company_id, cd.company_id) AS parent_id
    FROM adtech.company_developers AS cd
    LEFT JOIN store_apps AS sa ON cd.developer_id = sa.developer
    LEFT JOIN adtech.companies AS pc ON cd.company_id = pc.id
),

all_apps_with_companies AS (
    SELECT
        sawc.store_app,
        sawc.company_id,
        sawc.parent_id
    FROM sdk_apps_with_companies AS sawc
    UNION
    SELECT
        spwc.store_app,
        spwc.company_id,
        spwc.parent_id
    FROM sdk_paths_with_companies AS spwc
    UNION
    SELECT
        dawc.store_app,
        dawc.company_id,
        dawc.parent_id
    FROM dev_apps_with_companies AS dawc
),

distinct_apps_with_cats AS (
    SELECT DISTINCT
        aawc.store_app,
        c.id AS category_id
    FROM all_apps_with_companies AS aawc
    LEFT JOIN adtech.company_categories AS cc ON aawc.company_id = cc.company_id
    LEFT JOIN adtech.categories AS c ON cc.category_id = c.id
),

distinct_store_apps AS (
    SELECT DISTINCT lvc.store_app
    FROM latest_version_codes AS lvc
),

all_combinations AS (
    SELECT
        sa.store_app,
        c.id AS category_id
    FROM distinct_store_apps AS sa
    CROSS JOIN adtech.categories AS c
),

unmatched_apps AS (
    SELECT DISTINCT
        ac.store_app,
        -ac.category_id AS company_id,
        -ac.category_id AS parent_id
    FROM all_combinations AS ac
    LEFT JOIN
        distinct_apps_with_cats AS dawc
        ON ac.store_app = dawc.store_app AND ac.category_id = dawc.category_id
    WHERE dawc.store_app IS NULL
),

final_union AS (
    SELECT
        aawc.store_app,
        aawc.company_id,
        aawc.parent_id
    FROM all_apps_with_companies AS aawc
    UNION
    SELECT
        ua.store_app,
        ua.company_id,
        ua.parent_id
    FROM unmatched_apps AS ua
)

SELECT
    store_app,
    company_id,
    parent_id
FROM final_union
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX idx_store_apps_companies_sdk_new ON adtech.store_apps_companies_sdk_new USING btree (
    store_app, company_id, parent_id
);

CREATE MATERIALIZED VIEW r
TABLESPACE pg_default
AS
SELECT
    vd.id AS version_string_id,
    sd.company_id,
    sp.sdk_id AS id
FROM version_strings AS vd
INNER JOIN
    adtech.sdk_packages AS sp
    ON vd.value_name ~~* (sp.package_pattern::text || '%'::text)
LEFT JOIN adtech.sdks AS sd ON sp.sdk_id = sd.id
WITH DATA;

-- View indexes:
CREATE INDEX company_value_string_mapping_idx ON adtech.company_value_string_mapping USING btree (
    company_id
);

CREATE MATERIALIZED VIEW frontend.store_apps_version_details
TABLESPACE pg_default
AS WITH latest_version_codes AS (
    SELECT DISTINCT ON (version_codes.store_app)
        version_codes.id,
        version_codes.store_app,
        version_codes.version_code,
        version_codes.updated_at,
        version_codes.crawl_result
    FROM version_codes
    ORDER BY
        version_codes.store_app,
        (
            string_to_array(
                version_codes.version_code::text, '.'::text
            )::bigint []
        ) DESC
)

SELECT DISTINCT
    vs.id AS version_string_id,
    sa.store,
    sa.store_id,
    cvsm.company_id,
    c.name AS company_name,
    ad.domain AS company_domain,
    cats.url_slug AS category_slug
FROM latest_version_codes AS vc
LEFT JOIN version_details_map AS vdm ON vc.id = vdm.version_code
LEFT JOIN version_strings AS vs ON vdm.string_id = vs.id
LEFT JOIN
    adtech.company_value_string_mapping AS cvsm
    ON vs.id = cvsm.version_string_id
LEFT JOIN adtech.companies AS c ON cvsm.company_id = c.id
LEFT JOIN adtech.company_categories AS cc ON c.id = cc.company_id
LEFT JOIN adtech.categories AS cats ON cc.category_id = cats.id
LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
LEFT JOIN store_apps AS sa ON vc.store_app = sa.id
WITH DATA;

-- View indexes:
CREATE INDEX store_apps_version_details_store_id_idx ON frontend.store_apps_version_details USING btree (
    store_id
);
CREATE UNIQUE INDEX store_apps_version_details_unique_idx ON frontend.store_apps_version_details USING btree (
    version_string_id, store_id, company_id, company_domain, category_slug
);


CREATE MATERIALIZED VIEW frontend.companies_version_details_count
TABLESPACE pg_default
AS SELECT
    savd.store,
    savd.company_name,
    savd.company_domain,
    vs.xml_path,
    vs.value_name,
    count(DISTINCT savd.store_id) AS app_count
FROM frontend.store_apps_version_details AS savd
LEFT JOIN version_strings AS vs ON savd.version_string_id = vs.id
GROUP BY
    savd.store,
    savd.company_name,
    savd.company_domain,
    vs.xml_path,
    vs.value_name
ORDER BY (count(DISTINCT savd.store_id)) DESC
LIMIT 1000
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX companies_apps_version_details_count_unique_idx ON frontend.companies_version_details_count USING btree (
    store, company_name, company_domain, xml_path, value_name
);


CREATE MATERIALIZED VIEW frontend.companies_apps_overview
TABLESPACE pg_default
AS SELECT DISTINCT
    store_id,
    company_id,
    company_name,
    company_domain,
    category_slug
FROM frontend.store_apps_version_details
WHERE company_id IS NOT NULL
WITH DATA;

-- View indexes:
CREATE INDEX companies_apps_overview_idx ON frontend.companies_apps_overview USING btree (
    store_id
);
CREATE UNIQUE INDEX companies_apps_overview_unique_idx ON frontend.companies_apps_overview USING btree (
    store_id, company_id, category_slug
);

CREATE MATERIALIZED VIEW frontend.store_apps_overview
TABLESPACE pg_default
AS
WITH
latest_version_codes AS (
    SELECT DISTINCT ON
    (store_app)
        id,
        store_app,
        version_code,
        updated_at AS last_downloaded_at,
        crawl_result AS download_result
    FROM
        version_codes
    WHERE
        crawl_result = 1
        -- HACKY FIX only try for apps that have successuflly been downloaded, but this table still is all history of version_codes in general
        AND updated_at >= '2025-05-01'
    ORDER BY
        store_app ASC,
        updated_at DESC,
        string_to_array(version_code, '.')::bigint [] DESC
),

latest_successful_version_codes AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vc.updated_at,
        vc.crawl_result
    FROM version_codes AS vc
    WHERE vc.crawl_result = 1
    ORDER BY
        vc.store_app,
        (string_to_array(vc.version_code::text, '.'::text)::bigint []) DESC
),

last_sdk_scan AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vc.store_app,
        version_code_id AS version_code,
        scanned_at,
        scan_result
    FROM
        version_code_sdk_scan_results AS lsscr
    LEFT JOIN version_codes AS vc ON lsscr.version_code_id = vc.id
    ORDER BY
        vc.store_app ASC,
        lsscr.scanned_at DESC
),

last_successful_sdk_scan AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vcss.scanned_at,
        vcss.scan_result
    FROM
        version_codes AS vc
    LEFT JOIN version_code_sdk_scan_results AS vcss
        ON
            vc.id = vcss.version_code_id
    WHERE vcss.scan_result = 1
    ORDER BY
        vc.store_app ASC,
        vcss.scanned_at DESC,
        string_to_array(vc.version_code, '.')::bigint [] DESC
),

latest_en_descriptions AS (
    SELECT DISTINCT ON (store_apps_descriptions.store_app)
        store_apps_descriptions.store_app,
        store_apps_descriptions.description,
        store_apps_descriptions.description_short
    FROM store_apps_descriptions
    WHERE store_apps_descriptions.language_id = 1
    ORDER BY
        store_apps_descriptions.store_app ASC,
        store_apps_descriptions.updated_at DESC
),

latest_api_calls AS (
    SELECT DISTINCT ON
    (vc.store_app)
        store_app,
        vasr.run_result,
        vasr.run_at
    FROM
        version_codes AS vc
    LEFT JOIN version_code_api_scan_results AS vasr
        ON vc.id = vasr.version_code_id
    WHERE
        vc.updated_at >= '2025-05-01'
),

latest_successful_api_calls AS (
    SELECT DISTINCT ON
    (vc.store_app)
        store_app,
        vasr.run_at
    FROM
        version_codes AS vc
    LEFT JOIN version_code_api_scan_results AS vasr
        ON vc.id = vasr.version_code_id
    WHERE
        vasr.run_result = 1
        AND vc.updated_at >= '2025-05-01'
)

SELECT
    sa.id,
    sa.name,
    sa.store_id,
    sa.store,
    sa.category,
    sa.rating,
    sa.rating_count,
    sa.review_count,
    sa.installs,
    saz.installs_sum_1w,
    saz.ratings_sum_1w,
    saz.installs_sum_4w,
    saz.ratings_sum_4w,
    saz.installs_z_score_2w,
    saz.ratings_z_score_2w,
    saz.installs_z_score_4w,
    saz.ratings_z_score_4w,
    sa.ad_supported,
    sa.in_app_purchases,
    sa.store_last_updated,
    sa.created_at,
    sa.updated_at,
    sa.crawl_result,
    sa.icon_url_512,
    sa.release_date,
    sa.featured_image_url,
    sa.phone_image_url_1,
    sa.phone_image_url_2,
    sa.phone_image_url_3,
    sa.tablet_image_url_1,
    sa.tablet_image_url_2,
    sa.tablet_image_url_3,
    d.developer_id,
    d.name AS developer_name,
    pd.url AS developer_url,
    pd.updated_at AS adstxt_last_crawled,
    pd.crawl_result AS adstxt_crawl_result,
    lss.scanned_at AS sdk_last_crawled,
    lss.scanned_at AS sdk_crawl_result,
    lsss.scanned_at AS sdk_successful_last_crawled,
    lvc.version_code,
    ld.description,
    ld.description_short,
    lac.run_at AS api_last_crawled,
    lac.run_result,
    lsac.run_at AS api_successful_last_crawled
FROM store_apps AS sa
LEFT JOIN developers AS d ON sa.developer = d.id
LEFT JOIN app_urls_map AS aum ON sa.id = aum.store_app
LEFT JOIN pub_domains AS pd ON aum.pub_domain = pd.id
LEFT JOIN latest_version_codes AS lvc ON sa.id = lvc.store_app
LEFT JOIN last_sdk_scan AS lss ON sa.id = lss.store_app
LEFT JOIN last_successful_sdk_scan AS lsss ON sa.id = lsss.store_app
LEFT JOIN latest_successful_version_codes AS lsvc ON sa.id = lsvc.store_app
LEFT JOIN latest_en_descriptions AS ld ON sa.id = ld.store_app
LEFT JOIN store_app_z_scores AS saz ON sa.id = saz.store_app
LEFT JOIN latest_api_calls AS lac ON sa.id = lac.store_app
LEFT JOIN latest_successful_api_calls AS lsac ON sa.id = lsac.store_app
WITH DATA;

CREATE INDEX store_apps_overview_idx ON frontend.store_apps_overview_new USING btree (
    store_id
);
CREATE UNIQUE INDEX store_apps_overview_unique_idx ON frontend.store_apps_overview USING btree (
    store, store_id
);


CREATE MATERIALIZED VIEW frontend.company_parent_top_apps
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS installs_d30,
        sum(sahw.rating_count_diff) AS rating_count_d30
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '30 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

ranked_apps AS (
    SELECT
        sa.store,
        csapc.tag_source,
        sa.name,
        sa.store_id,
        csapc.app_category AS category,
        sa.rating_count,
        sa.installs,
        dc.installs_d30,
        dc.rating_count_d30,
        csapc.ad_domain AS company_domain,
        c.name AS company_name,
        row_number()
            OVER (
                PARTITION BY sa.store, csapc.ad_domain, c.name, csapc.tag_source
                ORDER BY
                    (
                        greatest(
                            coalesce(
                                dc.rating_count_d30, 0::numeric
                            )::double precision,
                            coalesce(
                                dc.installs_d30::double precision,
                                0::double precision
                            )
                        )
                    ) DESC
            )
        AS app_company_rank,
        row_number()
            OVER (
                PARTITION BY
                    sa.store,
                    csapc.app_category,
                    csapc.ad_domain,
                    c.name,
                    csapc.tag_source
                ORDER BY
                    (
                        greatest(
                            coalesce(
                                dc.rating_count_d30, 0::numeric
                            )::double precision,
                            coalesce(
                                dc.installs_d30::double precision,
                                0::double precision
                            )
                        )
                    ) DESC
            )
        AS app_company_category_rank
    FROM adtech.combined_store_apps_parent_companies AS csapc
    LEFT JOIN store_apps AS sa ON csapc.store_app = sa.id
    LEFT JOIN adtech.companies AS c ON csapc.company_id = c.id
    LEFT JOIN d30_counts AS dc ON csapc.store_app = dc.store_app
)

SELECT
    company_domain,
    company_name,
    store,
    tag_source,
    name,
    store_id,
    category,
    app_company_rank,
    app_company_category_rank,
    installs_d30,
    rating_count_d30,
    rating_count,
    installs
FROM ranked_apps
WHERE app_company_category_rank <= 100
ORDER BY store, tag_source, app_company_category_rank
WITH DATA;


-- View indexes:
CREATE INDEX idx_company_parent_top_apps ON frontend.company_parent_top_apps USING btree (
    company_domain
);
CREATE INDEX idx_company_parent_top_apps_domain_rank ON frontend.company_parent_top_apps USING btree (
    company_domain, app_company_rank
);
CREATE UNIQUE INDEX idx_company_parent_top_apps_unique ON frontend.company_parent_top_apps USING btree (
    company_domain, company_name, store, tag_source, name, store_id, category
);


CREATE MATERIALIZED VIEW frontend.total_categories_app_counts
TABLESPACE pg_default
AS SELECT
    sa.store,
    csac.tag_source,
    csac.app_category,
    count(DISTINCT csac.store_app) AS app_count
FROM adtech.combined_store_apps_companies AS csac
LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
GROUP BY sa.store, csac.tag_source, csac.app_category
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX idx_total_categories_app_counts ON frontend.total_categories_app_counts USING btree (
    store, tag_source, app_category
);


CREATE MATERIALIZED VIEW frontend.company_top_apps
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS installs_d30,
        sum(sahw.rating_count_diff) AS rating_count_d30
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '30 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

ranked_apps AS (
    SELECT
        sa.store,
        cac.tag_source,
        sa.name,
        sa.store_id,
        cac.app_category AS category,
        dc.installs_d30,
        dc.rating_count_d30,
        sa.rating_count,
        sa.installs,
        cac.ad_domain AS company_domain,
        c.name AS company_name,
        row_number()
            OVER (
                PARTITION BY sa.store, cac.ad_domain, c.name, cac.tag_source
                ORDER BY
                    (
                        greatest(
                            coalesce(
                                dc.rating_count_d30, 0::numeric
                            )::double precision,
                            coalesce(
                                dc.installs_d30::double precision,
                                0::double precision
                            )
                        )
                    ) DESC
            )
        AS app_company_rank,
        row_number()
            OVER (
                PARTITION BY
                    sa.store,
                    cac.app_category,
                    cac.ad_domain,
                    c.name,
                    cac.tag_source
                ORDER BY
                    (
                        greatest(
                            coalesce(
                                dc.rating_count_d30, 0::numeric
                            )::double precision,
                            coalesce(
                                dc.installs_d30::double precision,
                                0::double precision
                            )
                        )
                    ) DESC
            )
        AS app_company_category_rank
    FROM adtech.combined_store_apps_companies AS cac
    LEFT JOIN store_apps AS sa ON cac.store_app = sa.id
    LEFT JOIN adtech.companies AS c ON cac.company_id = c.id
    LEFT JOIN d30_counts AS dc ON cac.store_app = dc.store_app
)

SELECT
    company_domain,
    company_name,
    store,
    tag_source,
    name,
    store_id,
    category,
    app_company_rank,
    app_company_category_rank,
    installs_d30,
    rating_count_d30,
    installs,
    rating_count
FROM ranked_apps
WHERE app_company_category_rank <= 100
ORDER BY store, tag_source, app_company_category_rank
WITH DATA;


-- View indexes:
CREATE INDEX idx_company_top_apps ON frontend.company_top_apps USING btree (
    company_domain
);
CREATE INDEX idx_company_top_apps_domain_rank ON frontend.company_top_apps USING btree (
    company_domain, app_company_rank
);
CREATE INDEX idx_query_company_top_apps ON frontend.company_top_apps USING btree (
    company_domain, category, app_company_category_rank, store, tag_source
);
CREATE UNIQUE INDEX idx_unique_company_top_apps ON frontend.company_top_apps USING btree (
    company_domain, company_name, store, tag_source, name, store_id, category
);


CREATE MATERIALIZED VIEW frontend.app_rankings_latest_by_week
TABLESPACE pg_default
AS WITH RECURSIVE latest_date AS (
    SELECT max(app_rankings.crawled_date) AS max_date
    FROM app_rankings
),

my_dates AS (
    SELECT ld.max_date AS crawled_date
    FROM latest_date AS ld
    UNION ALL
    SELECT (md.crawled_date - '7 days'::interval)::date AS date
    FROM my_dates AS md
    WHERE
        (md.crawled_date - '7 days'::interval)
        >= (current_date - '45 days'::interval)
),

top_apps_newer AS (
    SELECT DISTINCT ar.store_app
    FROM app_rankings AS ar
    INNER JOIN countries AS c_1 ON ar.country = c_1.id
    WHERE ar.crawled_date = ((
        SELECT ld.max_date
        FROM latest_date AS ld
    )) AND ar.rank <= 50
),

top_apps_older AS (
    SELECT DISTINCT ar.store_app
    FROM app_rankings AS ar
    INNER JOIN countries AS c_1 ON ar.country = c_1.id AND ar.rank <= 50
)

SELECT
    arr.crawled_date,
    arr.store_collection,
    arr.store_category,
    arr.rank,
    sa.store,
    c.alpha2 AS country,
    sa.name,
    sa.store_id
FROM app_rankings AS arr
LEFT JOIN store_apps AS sa ON arr.store_app = sa.id
LEFT JOIN countries AS c ON arr.country = c.id
WHERE (arr.store_app IN (
    SELECT tan.store_app
    FROM top_apps_newer AS tan
    UNION
    SELECT tao.store_app
    FROM top_apps_older AS tao
))
AND arr.crawled_date >= (current_date - '45 days'::interval)
AND (arr.crawled_date IN (
    SELECT md.crawled_date
    FROM my_dates AS md
))
WITH DATA;

-- View indexes:
CREATE INDEX idx_app_rankings_latest_by_week_query ON frontend.app_rankings_latest_by_week USING btree (
    store, store_collection, country, crawled_date
);
CREATE UNIQUE INDEX idx_unique_app_ranking_latest_by_week ON frontend.app_rankings_latest_by_week USING btree (
    crawled_date,
    store_collection,
    store_category,
    rank,
    store,
    country,
    store_id
);


CREATE INDEX idx_app_rankings_latest_by_week_query ON
frontend.app_rankings_latest_by_week USING btree (
    store, store_collection, country, crawled_date
);


CREATE MATERIALIZED VIEW frontend.category_tag_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        csac.tag_source,
        sa.installs,
        sa.rating_count
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
)

SELECT
    dag.store,
    dag.app_category,
    dag.tag_source,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM distinct_apps_group AS dag
LEFT JOIN d30_counts AS dc ON dag.store_app = dc.store_app
GROUP BY dag.store, dag.app_category, dag.tag_source
WITH DATA;


-- View indexes:
CREATE UNIQUE INDEX idx_category_tag_stats ON frontend.category_tag_stats USING btree (
    store, app_category, tag_source
);


CREATE MATERIALIZED VIEW frontend.companies_sdks_overview
TABLESPACE pg_default
AS SELECT
    c.name AS company_name,
    ad.domain AS company_domain,
    parad.domain AS parent_company_domain,
    sdk.sdk_name,
    sp.package_pattern,
    sp2.path_pattern,
    coalesce(cc.name, c.name) AS parent_company_name
FROM adtech.companies AS c
LEFT JOIN adtech.companies AS cc ON c.parent_company_id = cc.id
LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
LEFT JOIN ad_domains AS parad ON cc.domain_id = parad.id
LEFT JOIN adtech.sdks AS sdk ON c.id = sdk.company_id
LEFT JOIN adtech.sdk_packages AS sp ON sdk.id = sp.sdk_id
LEFT JOIN adtech.sdk_paths AS sp2 ON sdk.id = sp2.sdk_id
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX companies_sdks_overview_unique_idx ON frontend.companies_sdks_overview USING btree (
    company_name,
    company_domain,
    parent_company_domain,
    sdk_name,
    package_pattern,
    path_pattern
);


CREATE MATERIALIZED VIEW frontend.companies_open_source_percent
TABLESPACE pg_default
AS SELECT
    ad.domain AS company_domain,
    avg(
        CASE
            WHEN sd.is_open_source THEN 1
            ELSE 0
        END
    ) AS percent_open_source
FROM adtech.sdks AS sd
LEFT JOIN adtech.companies AS c ON sd.company_id = c.id
LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
GROUP BY ad.domain
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX companies_open_source_percent_unique ON frontend.companies_open_source_percent USING btree (
    company_domain
);


DROP MATERIALIZED VIEW frontend.latest_sdk_scanned_apps;
CREATE MATERIALIZED VIEW frontend.latest_sdk_scanned_apps
AS
WITH latest_version_codes AS (
    SELECT DISTINCT ON
    (version_codes.store_app)
        version_codes.id,
        version_codes.store_app,
        version_codes.version_code,
        version_codes.updated_at,
        version_codes.crawl_result
    FROM
        version_codes
    ORDER BY
        version_codes.store_app,
        (
            string_to_array(
                version_codes.version_code::text,
                '.'::text
            )::bigint []
        ) DESC
),

ranked_apps AS (
    SELECT
        lvc.updated_at AS sdk_crawled_at,
        lvc.version_code,
        lvc.crawl_result,
        sa.store,
        sa.store_id,
        sa.name,
        sa.installs,
        sa.rating_count,
        row_number() OVER (
            PARTITION BY sa.store, lvc.crawl_result
            ORDER BY
                lvc.updated_at DESC
        ) AS updated_rank
    FROM
        latest_version_codes AS lvc
    LEFT JOIN store_apps AS sa
        ON
            lvc.store_app = sa.id
    WHERE
        lvc.updated_at <= current_date - interval '1 day'
)

SELECT *
FROM
    ranked_apps AS ra
WHERE
    ra.updated_rank <= 100
WITH DATA;


CREATE UNIQUE INDEX latest_sdk_scanned_apps_unique_index
ON
frontend.latest_sdk_scanned_apps
USING btree (
    version_code,
    crawl_result,
    store,
    store_id
);


CREATE MATERIALIZED VIEW frontend.adstxt_entries_store_apps
TABLESPACE pg_default
AS WITH parent_companies AS (
    SELECT
        c.id AS company_id,
        c.name AS company_name,
        coalesce(c.parent_company_id, c.id) AS parent_company_id,
        coalesce(pc.name, c.name) AS parent_company_name,
        coalesce(pc.domain_id, c.domain_id) AS parent_company_domain_id
    FROM adtech.companies AS c
    LEFT JOIN adtech.companies AS pc ON c.parent_company_id = pc.id
)

SELECT DISTINCT
    ad.id AS ad_domain_id,
    myc.parent_company_id AS company_id,
    aae.id AS app_ad_entry_id,
    sa.id AS store_app,
    pd.id AS pub_domain_id
FROM app_ads_entrys AS aae
LEFT JOIN ad_domains AS ad ON aae.ad_domain = ad.id
LEFT JOIN app_ads_map AS aam ON aae.id = aam.app_ads_entry
LEFT JOIN pub_domains AS pd ON aam.pub_domain = pd.id
LEFT JOIN app_urls_map AS aum ON pd.id = aum.pub_domain
INNER JOIN store_apps AS sa ON aum.store_app = sa.id
LEFT JOIN parent_companies AS myc ON ad.id = myc.parent_company_domain_id
WHERE (pd.crawled_at - aam.updated_at) < '01:00:00'::interval
WITH DATA;

-- View indexes:
CREATE INDEX adstxt_entries_store_apps_domain_pub_idx ON frontend.adstxt_entries_store_apps USING btree (
    ad_domain_id, app_ad_entry_id
);
CREATE INDEX adstxt_entries_store_apps_idx ON frontend.adstxt_entries_store_apps USING btree (
    store_app
);
CREATE UNIQUE INDEX adstxt_entries_store_apps_unique_idx ON frontend.adstxt_entries_store_apps USING btree (
    ad_domain_id, app_ad_entry_id, store_app
);


CREATE MATERIALIZED VIEW frontend.adstxt_publishers_overview
TABLESPACE pg_default
AS WITH ranked_data AS (
    SELECT
        ad.domain AS ad_domain_url,
        aae.relationship,
        sa.store,
        aae.publisher_id,
        count(DISTINCT sa.developer) AS developer_count,
        count(DISTINCT aesa.store_app) AS app_count,
        row_number()
            OVER (
                PARTITION BY ad.domain, aae.relationship, sa.store
                ORDER BY (count(DISTINCT aesa.store_app)) DESC
            )
        AS pubrank
    FROM frontend.adstxt_entries_store_apps AS aesa
    LEFT JOIN store_apps AS sa ON aesa.store_app = sa.id
    LEFT JOIN app_ads_entrys AS aae ON aesa.app_ad_entry_id = aae.id
    LEFT JOIN ad_domains AS ad ON aesa.ad_domain_id = ad.id
    GROUP BY ad.domain, aae.relationship, sa.store, aae.publisher_id
)

SELECT
    ad_domain_url,
    relationship,
    store,
    publisher_id,
    developer_count,
    app_count,
    pubrank
FROM ranked_data
WHERE pubrank <= 50
WITH DATA;

-- View indexes:
CREATE INDEX adstxt_publishers_overview_ad_domain_idx ON frontend.adstxt_publishers_overview USING btree (
    ad_domain_url
);
CREATE UNIQUE INDEX adstxt_publishers_overview_ad_domain_unique_idx ON frontend.adstxt_publishers_overview USING btree (
    ad_domain_url, relationship, store, publisher_id
);


-- frontend.adstxt_ad_domain_overview source

CREATE MATERIALIZED VIEW frontend.adstxt_ad_domain_overview
TABLESPACE pg_default
AS SELECT
    ad.domain AS ad_domain_url,
    aae.relationship,
    sa.store,
    count(DISTINCT aae.publisher_id) AS publisher_id_count,
    count(DISTINCT sa.developer) AS developer_count,
    count(DISTINCT aesa.store_app) AS app_count
FROM frontend.adstxt_entries_store_apps AS aesa
LEFT JOIN store_apps AS sa ON aesa.store_app = sa.id
LEFT JOIN app_ads_entrys AS aae ON aesa.app_ad_entry_id = aae.id
LEFT JOIN ad_domains AS ad ON aesa.ad_domain_id = ad.id
GROUP BY ad.domain, aae.relationship, sa.store
WITH DATA;

-- View indexes:
CREATE INDEX adstxt_ad_domain_overview_idx ON frontend.adstxt_ad_domain_overview USING btree (
    ad_domain_url
);
CREATE UNIQUE INDEX adstxt_ad_domain_overview_unique_idx ON frontend.adstxt_ad_domain_overview USING btree (
    ad_domain_url, relationship, store
);


CREATE MATERIALIZED VIEW frontend.store_app_ranks_weekly
TABLESPACE pg_default
AS
WITH weekly_max_dates AS (
    SELECT
        ar.country,
        ar.store_collection,
        ar.store_category,
        date_trunc(
            'week'::text, ar.crawled_date::timestamp with time zone
        ) AS week_start,
        min(ar.rank) AS best_rank,
        max(ar.crawled_date) AS max_crawled_date
    FROM app_rankings AS ar
    WHERE ar.crawled_date >= (current_date - '120 days'::interval)
    GROUP BY
        ar.country,
        ar.store_collection,
        ar.store_category,
        (date_trunc('week'::text, ar.crawled_date::timestamp with time zone))
)

SELECT
    ar.rank,
    wmd.best_rank,
    ar.country,
    ar.store_collection,
    ar.store_category,
    ar.crawled_date,
    ar.store_app
FROM app_rankings AS ar
INNER JOIN
    weekly_max_dates AS wmd
    ON
        ar.country = wmd.country
        AND ar.store_collection = wmd.store_collection
        AND ar.store_category = wmd.store_category
        AND ar.crawled_date = wmd.max_crawled_date
WHERE ar.crawled_date >= (current_date - '120 days'::interval)
ORDER BY
    ar.country, ar.store_collection, ar.store_category, ar.crawled_date, ar.rank
WITH DATA;

-- View indexes:
CREATE INDEX idx_store_app_ranks_weekly_query ON frontend.store_app_ranks_weekly USING btree (
    store_collection, store_category, country, crawled_date, rank
);
CREATE UNIQUE INDEX idx_store_app_ranks_weekly_unique ON frontend.store_app_ranks_weekly USING btree (
    store_collection, store_category, country, crawled_date, rank, store_app
);


CREATE MATERIALIZED VIEW frontend.companies_category_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        csac.ad_domain AS company_domain,
        c.name AS company_name,
        sa.installs,
        sa.rating_count
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c ON csac.company_id = c.id
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
)

SELECT
    dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM distinct_apps_group AS dag
LEFT JOIN d30_counts AS dc ON dag.store_app = dc.store_app
GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
WITH DATA;


-- View indexes:
CREATE UNIQUE INDEX companies_category_stats_idx ON frontend.companies_category_stats USING btree (
    store, app_category, company_domain
);
CREATE INDEX companies_category_stats_query_idx ON frontend.companies_category_stats USING btree (
    company_domain
);


-- frontend.companies_category_tag_stats source

CREATE MATERIALIZED VIEW frontend.companies_category_tag_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        csac.tag_source,
        csac.ad_domain AS company_domain,
        c.name AS company_name,
        sa.installs,
        sa.rating_count
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c ON csac.company_id = c.id
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
)

SELECT
    dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM distinct_apps_group AS dag
LEFT JOIN d30_counts AS dc ON dag.store_app = dc.store_app
GROUP BY
    dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name
WITH DATA;


-- View indexes:
CREATE INDEX companies_category_tag_stats__query_idx ON frontend.companies_category_tag_stats USING btree (
    company_domain
);
CREATE UNIQUE INDEX companies_category_tag_stats_idx ON frontend.companies_category_tag_stats USING btree (
    store, tag_source, app_category, company_domain
);


CREATE MATERIALIZED VIEW frontend.companies_category_tag_type_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
)

SELECT
    sa.store,
    csac.app_category,
    csac.tag_source,
    csac.ad_domain AS company_domain,
    c.name AS company_name,
    CASE
        WHEN
            csac.tag_source ~~ 'app_ads%'::text
            THEN 'ad-networks'::character varying
        ELSE cats.url_slug
    END AS type_url_slug,
    count(DISTINCT csac.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(sa.installs) AS installs_total,
    sum(sa.rating_count) AS rating_count_total
FROM adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.companies AS c ON csac.company_id = c.id
LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
LEFT JOIN d30_counts AS dc ON csac.store_app = dc.store_app
LEFT JOIN
    adtech.company_categories AS ccats
    ON csac.company_id = ccats.company_id
LEFT JOIN adtech.categories AS cats ON ccats.category_id = cats.id
GROUP BY
    sa.store, csac.app_category, csac.tag_source, csac.ad_domain, c.name, (
        CASE
            WHEN
                csac.tag_source ~~ 'app_ads%'::text
                THEN 'ad-networks'::character varying
            ELSE cats.url_slug
        END
    )
WITH DATA;


-- View indexes:
CREATE UNIQUE INDEX companies_category_tag_type_stats_idx ON frontend.companies_category_tag_type_stats USING btree (
    store, tag_source, app_category, company_domain, type_url_slug
);
CREATE INDEX companies_category_tag_type_stats_query_idx ON frontend.companies_category_tag_type_stats USING btree (
    type_url_slug, app_category
);


CREATE MATERIALIZED VIEW frontend.companies_parent_category_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        c.name AS company_name,
        sa.installs,
        sa.rating_count,
        coalesce(ad.domain, csac.ad_domain) AS company_domain
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c ON csac.parent_id = c.id
    LEFT JOIN adtech.company_domain_mapping AS cdm ON c.id = cdm.company_id
    LEFT JOIN ad_domains AS ad ON cdm.domain_id = ad.id
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
    WHERE csac.parent_id IN (
        SELECT DISTINCT pc.id
        FROM
            adtech.companies AS pc
        LEFT JOIN adtech.companies AS c
            ON
                pc.id = c.parent_company_id
        WHERE
            c.id IS NOT NULL
    )
)

SELECT
    dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM distinct_apps_group AS dag
LEFT JOIN d30_counts AS dc ON dag.store_app = dc.store_app
GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX companies_parent_category_stats_idx ON frontend.companies_parent_category_stats USING btree (
    store, company_domain, company_name, app_category
);
CREATE INDEX companies_parent_category_stats_query_idx ON frontend.companies_parent_category_stats USING btree (
    company_domain
);


CREATE MATERIALIZED VIEW frontend.companies_parent_category_tag_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        csac.tag_source,
        c.name AS company_name,
        sa.installs,
        sa.rating_count,
        coalesce(ad.domain, csac.ad_domain) AS company_domain
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c ON csac.parent_id = c.id
    LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
    WHERE (csac.parent_id IN (
        SELECT DISTINCT pc.id
        FROM adtech.companies AS pc
        LEFT JOIN adtech.companies AS c_1 ON pc.id = c_1.parent_company_id
        WHERE c_1.id IS NOT NULL
    ))
)

SELECT
    dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM distinct_apps_group AS dag
LEFT JOIN d30_counts AS dc ON dag.store_app = dc.store_app
GROUP BY
    dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX companies_parent_category_tag_stats_idx ON frontend.companies_parent_category_tag_stats USING btree (
    store, company_domain, company_name, app_category, tag_source
);
CREATE INDEX companies_parent_category_tag_stats_query_idx ON frontend.companies_parent_category_tag_stats USING btree (
    company_domain
);


CREATE MATERIALIZED VIEW frontend.companies_parent_category_stats
TABLESPACE pg_default
AS WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM store_apps_history_weekly AS sahw
    WHERE
        sahw.week_start > (current_date - '31 days'::interval)
        AND sahw.country_id = 840
        AND (sahw.installs_diff > 0::numeric OR sahw.rating_count_diff > 0)
    GROUP BY sahw.store_app
),

distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        c.name AS company_name,
        sa.installs,
        sa.rating_count,
        coalesce(ad.domain, csac.ad_domain) AS company_domain
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c ON csac.parent_id = c.id
    LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
    WHERE (csac.parent_id IN (
        SELECT DISTINCT pc.id
        FROM adtech.companies AS pc
        LEFT JOIN adtech.companies AS c_1 ON pc.id = c_1.parent_company_id
        WHERE c_1.id IS NOT NULL
    ))
)

SELECT
    dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM distinct_apps_group AS dag
LEFT JOIN d30_counts AS dc ON dag.store_app = dc.store_app
GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
WITH DATA;


-- View indexes:
CREATE UNIQUE INDEX companies_parent_category_stats_idx ON frontend.companies_parent_category_stats USING btree (
    store, company_domain, company_name, app_category
);
CREATE INDEX companies_parent_category_stats_query_idx ON frontend.companies_parent_category_stats USING btree (
    company_domain
);


CREATE MATERIALIZED VIEW frontend.keyword_scores AS
WITH latest_en_descriptions AS (
    SELECT DISTINCT ON
    (store_app)
        sad.store_app,
        sad.id AS description_id
    FROM
        store_apps_descriptions AS sad
    INNER JOIN description_keywords AS dk
        ON
            sad.id = dk.description_id
    WHERE
        sad.language_id = 1
    ORDER BY
        sad.store_app ASC,
        sad.updated_at DESC
),

keyword_app_counts AS (
    SELECT
        sa.store,
        k.keyword_text,
        dk.keyword_id,
        count(DISTINCT led.store_app) AS app_count
    FROM
        latest_en_descriptions AS led
    LEFT JOIN description_keywords AS dk
        ON
            led.description_id = dk.description_id
    LEFT JOIN keywords AS k
        ON
            dk.keyword_id = k.id
    LEFT JOIN store_apps AS sa
        ON
            led.store_app = sa.id
    WHERE
        dk.keyword_id IS NOT NULL
    GROUP BY
        sa.store,
        k.keyword_text,
        dk.keyword_id
),

total_app_count AS (
    SELECT
        sa.store,
        count(*) AS total_apps
    FROM
        latest_en_descriptions AS led
    LEFT JOIN store_apps AS sa
        ON
            led.store_app = sa.id
    GROUP BY sa.store
)

SELECT
    kac.store,
    kac.keyword_text,
    kac.keyword_id,
    kac.app_count,
    tac.total_apps,
    round(
        100
        * (
            1
            - ln(tac.total_apps::float / (kac.app_count + 1))
            / ln(tac.total_apps::float)
        )::numeric,
        2
    ) AS competitiveness_score
FROM
    keyword_app_counts AS kac
LEFT JOIN total_app_count AS tac
    ON kac.store = tac.store
ORDER BY
    competitiveness_score DESC
WITH DATA;

CREATE UNIQUE INDEX keyword_scores_unique ON frontend.keyword_scores USING btree (
    store, keyword_id
)

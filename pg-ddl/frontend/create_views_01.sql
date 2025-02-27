DROP MATERIALIZED VIEW IF EXISTS frontend.companies_apps_version_details;
CREATE MATERIALIZED VIEW frontend.companies_apps_version_details
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
    vd.xml_path,
    vd.value_name,
    sa.store,
    sa.store_id,
    vnpm.company_id,
    c.name AS company_name,
    ad.domain AS company_domain,
    cats.url_slug AS category_slug
FROM latest_version_codes AS vc
LEFT JOIN version_details AS vd ON vc.id = vd.version_code
LEFT JOIN
    adtech.company_value_name_package_mapping AS vnpm
    ON vd.value_name = vnpm.value_name
LEFT JOIN adtech.companies AS c ON vnpm.company_id = c.id
LEFT JOIN adtech.company_categories AS cc ON c.id = cc.company_id
LEFT JOIN adtech.categories AS cats ON cc.category_id = cats.id
LEFT JOIN adtech.company_domain_mapping AS cdm ON c.id = cdm.company_id
LEFT JOIN ad_domains AS ad ON cdm.domain_id = ad.id
LEFT JOIN store_apps AS sa ON vc.store_app = sa.id
WITH DATA;


-- View indexes:
CREATE INDEX companies_apps_version_details_store_id_idx ON frontend.companies_apps_version_details USING btree (
    store_id
);
CREATE UNIQUE INDEX companies_apps_version_details_unique_idx
ON frontend.companies_apps_version_details USING btree (
    xml_path,
    value_name,
    store,
    store_id,
    company_id,
    company_domain,
    category_slug
);



DROP MATERIALIZED VIEW IF EXISTS frontend.companies_version_details_count;
CREATE MATERIALIZED VIEW frontend.companies_version_details_count
TABLESPACE pg_default
AS SELECT
    store,
    company_name,
    company_domain,
    xml_path,
    value_name,
    count(DISTINCT store_id) AS app_count
FROM frontend.companies_apps_version_details
GROUP BY store, company_name, company_domain, xml_path, value_name
WITH DATA;


CREATE UNIQUE INDEX companies_apps_version_details_count_unique_idx ON
frontend.companies_version_details_count (
    store, company_name, company_domain, xml_path, value_name
);


DROP MATERIALIZED VIEW IF EXISTS frontend.companies_apps_overview;
CREATE MATERIALIZED VIEW frontend.companies_apps_overview
TABLESPACE pg_default
AS SELECT DISTINCT
    store_id,
    company_id,
    company_name,
    company_domain,
    category_slug
FROM frontend.companies_apps_version_details
WHERE company_id IS NOT NULL
WITH DATA;


CREATE UNIQUE INDEX companies_apps_overview_unique_idx ON
frontend.companies_apps_overview USING btree (
    store_id, company_id, category_slug
);


CREATE INDEX companies_apps_overview_idx ON frontend.companies_apps_overview USING btree (
    store_id
);


DROP MATERIALIZED VIEW IF EXISTS frontend.store_apps_overview;
CREATE MATERIALIZED VIEW frontend.store_apps_overview
TABLESPACE pg_default
AS WITH latest_version_codes AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vc.updated_at,
        vc.crawl_result
    FROM version_codes AS vc
    ORDER BY
        vc.store_app,
        (string_to_array(vc.version_code::text, '.'::text)::bigint []) DESC
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
    lvc.updated_at AS sdk_last_crawled,
    lvc.crawl_result AS sdk_crawl_result,
    lsvc.updated_at AS sdk_successful_last_crawled,
    lvc.version_code
FROM store_apps AS sa
LEFT JOIN developers AS d ON sa.developer = d.id
LEFT JOIN app_urls_map AS aum ON sa.id = aum.store_app
LEFT JOIN pub_domains AS pd ON aum.pub_domain = pd.id
LEFT JOIN latest_version_codes AS lvc ON sa.id = lvc.store_app
LEFT JOIN latest_successful_version_codes AS lsvc ON sa.id = lsvc.store_app
WITH DATA;

-- View indexes:
CREATE INDEX store_apps_overview_idx ON frontend.store_apps_overview USING btree (
    store_id
);
CREATE UNIQUE INDEX store_apps_overview_unique_idx ON frontend.store_apps_overview USING btree (
    store, store_id
);



DROP MATERIALIZED VIEW IF EXISTS frontend.companies_parent_app_counts;
CREATE MATERIALIZED VIEW frontend.companies_parent_app_counts
TABLESPACE pg_default
AS
SELECT
    sa.store,
    cm.mapped_category AS app_category,
    csac.tag_source,
    csac.ad_domain AS company_domain,
    c.name AS company_name,
    count(*) AS app_count
FROM
    adtech.combined_store_apps_parent_companies AS csac
LEFT JOIN adtech.companies AS c
    ON
        csac.company_id = c.id
LEFT JOIN store_apps AS sa
    ON
        csac.store_app = sa.id
LEFT JOIN category_mapping AS cm
    ON
        sa.category::text = cm.original_category::text
GROUP BY
    sa.store,
    cm.mapped_category,
    csac.tag_source,
    csac.ad_domain,
    c.name
ORDER BY app_count DESC
WITH DATA;

-- View indexes:
CREATE INDEX idx_companies_parent_app_counts ON frontend.companies_parent_app_counts USING btree (
    app_category, tag_source
);

CREATE UNIQUE INDEX idx_unique_company_parent_app_counts ON
frontend.companies_parent_app_counts
USING btree (
    store,
    app_category,
    tag_source,
    company_domain
);


DROP MATERIALIZED VIEW IF EXISTS frontend.companies_categories_types_app_counts;
CREATE MATERIALIZED VIEW frontend.companies_categories_types_app_counts
TABLESPACE pg_default
AS
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
    count(DISTINCT csac.store_app) AS app_count
FROM
    adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.company_categories AS ccats
    ON
        csac.company_id = ccats.company_id
LEFT JOIN adtech.categories AS cats
    ON
        ccats.category_id = cats.id
LEFT JOIN adtech.companies AS c
    ON
        csac.company_id = c.id
LEFT JOIN store_apps AS sa
    ON
        csac.store_app = sa.id
GROUP BY
    sa.store,
    csac.app_category,
    csac.tag_source,
    csac.ad_domain,
    c.name,
    CASE
        WHEN
            csac.tag_source ~~ 'app_ads%'::text
            THEN 'ad-networks'::character varying
        ELSE cats.url_slug
    END
WITH DATA;


CREATE UNIQUE INDEX idx_unique_companies_categories_types_app_counts ON frontend.companies_categories_types_app_counts USING btree (
    store, tag_source, app_category, company_domain, type_url_slug
);

CREATE INDEX idx_companies_categories_types_app_counts ON frontend.companies_categories_types_app_counts USING btree (
    type_url_slug, app_category
);

CREATE INDEX idx_companies_categories_types_app_counts_types ON frontend.companies_categories_types_app_counts USING btree (
    type_url_slug
);


DROP MATERIALIZED VIEW IF EXISTS frontend.company_parent_top_apps;
CREATE MATERIALIZED VIEW frontend.company_parent_top_apps
TABLESPACE pg_default
AS
WITH ranked_apps AS (
    SELECT
        sa.store,
        csapc.tag_source,
        sa.name,
        sa.store_id,
        csapc.app_category AS category,
        sa.rating_count,
        sa.installs,
        csapc.ad_domain AS company_domain,
        c.name AS company_name,
        row_number() OVER (
            PARTITION BY
                sa.store,
                csapc.ad_domain,
                c.name,
                csapc.tag_source
            ORDER BY
                (
                    greatest(
                        coalesce(
                            sa.rating_count,
                            0
                        )::double precision,
                        coalesce(
                            sa.installs,
                            0::double precision
                        )
                    )
                ) DESC
        ) AS app_company_rank,
        row_number() OVER (
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
                            sa.rating_count,
                            0
                        )::double precision,
                        coalesce(
                            sa.installs,
                            0::double precision
                        )
                    )
                ) DESC
        ) AS app_company_category_rank
    FROM
        adtech.combined_store_apps_parent_companies AS csapc
    LEFT JOIN store_apps AS sa
        ON
            csapc.store_app = sa.id
    LEFT JOIN adtech.companies AS c ON
        csapc.company_id = c.id
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
    rating_count,
    installs
FROM
    ranked_apps
WHERE
    app_company_category_rank <= 100
ORDER BY
    store,
    tag_source,
    app_company_category_rank
WITH DATA;


CREATE UNIQUE INDEX idx_company_parent_top_apps ON
frontend.company_parent_top_apps
USING btree (
    company_domain,
    company_name,
    store,
    tag_source,
    name,
    store_id,
    category
);



DROP MATERIALIZED VIEW IF EXISTS frontend.company_top_apps;
CREATE MATERIALIZED VIEW frontend.company_top_apps
TABLESPACE pg_default
AS WITH ranked_apps AS (
    SELECT
        sa.store,
        cac.tag_source,
        sa.name,
        sa.store_id,
        cac.app_category AS category,
        sa.rating_count,
        sa.installs,
        cac.ad_domain AS company_domain,
        c.name AS company_name,
        row_number() OVER (
            PARTITION BY
                sa.store,
                cac.ad_domain,
                c.name,
                cac.tag_source
            ORDER BY
                (
                    greatest(
                        coalesce(
                            sa.rating_count,
                            0
                        )::double precision,
                        coalesce(
                            sa.installs,
                            0::double precision
                        )
                    )
                ) DESC
        ) AS app_company_rank,
        row_number() OVER (
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
                            sa.rating_count,
                            0
                        )::double precision,
                        coalesce(
                            sa.installs,
                            0::double precision
                        )
                    )
                ) DESC
        ) AS app_company_category_rank
    FROM
        adtech.combined_store_apps_companies AS cac
    LEFT JOIN store_apps AS sa
        ON
            cac.store_app = sa.id
    LEFT JOIN adtech.companies AS c ON
        cac.company_id = c.id
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
    rating_count,
    installs
FROM
    ranked_apps
WHERE
    app_company_category_rank <= 100
ORDER BY
    store,
    tag_source,
    app_company_category_rank
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX idx_unique_company_top_apps ON
frontend.company_top_apps
USING btree (
    company_domain,
    company_name,
    store,
    tag_source,
    name,
    store_id,
    category
);

CREATE INDEX idx_company_top_apps ON frontend.company_top_apps USING btree (
    company_domain
);

CREATE INDEX idx_query_company_top_apps
ON
frontend.company_top_apps (
    company_domain,
    category,
    app_company_category_rank,
    store,
    tag_source
);



DROP MATERIALIZED VIEW IF EXISTS frontend.app_rankings_latest_by_week;
CREATE MATERIALIZED VIEW frontend.app_rankings_latest_by_week
AS
WITH RECURSIVE latest_date AS (
    SELECT max(crawled_date) AS max_date
    FROM
        app_rankings
),

my_dates AS (
    SELECT ld.max_date AS crawled_date
    FROM
        latest_date AS ld
    UNION ALL
    SELECT
        (
            md.crawled_date - interval '7 days'
        )::date
    FROM
        my_dates AS md
    WHERE
        md.crawled_date - interval '7 days' >= current_date - interval '45 days'
),

top_apps_newer AS (
    SELECT DISTINCT ar.store_app
    FROM
        app_rankings AS ar
    INNER JOIN countries AS c
        ON
            ar.country = c.id
    WHERE
        ar.crawled_date = (
            SELECT ld.max_date
            FROM
                latest_date AS ld
        )
        AND ar.rank <= 50
),

top_apps_older AS (
    SELECT DISTINCT ar.store_app
    FROM
        app_rankings AS ar
    INNER JOIN countries AS c ON
        ar.country = c.id
        AND ar.rank <= 50
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
FROM
    app_rankings AS arr
LEFT JOIN store_apps AS sa
    ON
        arr.store_app = sa.id
LEFT JOIN countries AS c
    ON
        arr.country = c.id
WHERE
    arr.store_app IN (
        SELECT tan.store_app
        FROM
            top_apps_newer AS tan
        UNION
        SELECT tao.store_app
        FROM
            top_apps_older AS tao
    )
    AND arr.crawled_date >= current_date - interval '45 days'
    AND arr.crawled_date IN (
        SELECT md.crawled_date
        FROM
            my_dates AS md
    );

CREATE UNIQUE INDEX idx_unique_app_ranking_latest_by_week ON
frontend.app_rankings_latest_by_week
USING btree (
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



DROP MATERIALIZED VIEW IF EXISTS frontend.companies_app_counts;
CREATE MATERIALIZED VIEW frontend.companies_app_counts
TABLESPACE pg_default
AS WITH my_counts AS (
    SELECT DISTINCT
        csac.store_app,
        sa.store,
        cm.mapped_category AS app_category,
        csac.tag_source,
        csac.ad_domain AS company_domain,
        c.name AS company_name
    FROM adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c ON csac.company_id = c.id
    LEFT JOIN store_apps AS sa ON csac.store_app = sa.id
    LEFT JOIN
        category_mapping AS cm
        ON sa.category::text = cm.original_category::text
),

app_counts AS (
    SELECT
        my_counts.store,
        my_counts.app_category,
        my_counts.tag_source,
        my_counts.company_domain,
        my_counts.company_name,
        count(*) AS app_count
    FROM my_counts
    GROUP BY
        my_counts.store,
        my_counts.app_category,
        my_counts.tag_source,
        my_counts.company_domain,
        my_counts.company_name
)

SELECT
    app_count,
    store,
    app_category,
    tag_source,
    company_domain,
    company_name
FROM app_counts
ORDER BY app_count DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX idx_companies_app_counts ON frontend.companies_app_counts USING btree (
    store, app_category, tag_source, company_domain, company_name
);



DROP MATERIALIZED VIEW IF EXISTS frontend.companies_categories_app_counts;
CREATE MATERIALIZED VIEW frontend.companies_categories_app_counts
TABLESPACE pg_default
AS SELECT
    csac.ad_domain AS company_domain,
    c.name AS company_name,
    csac.app_category,
    count(DISTINCT csac.store_app) AS app_count
FROM adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.companies AS c ON csac.company_id = c.id
GROUP BY csac.ad_domain, c.name, csac.app_category
ORDER BY c.name, (count(DISTINCT csac.store_app)) DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX idx_companies_categories_app_counts ON frontend.companies_categories_app_counts USING btree (
    company_domain, company_name, app_category
);


DROP MATERIALIZED VIEW IF EXISTS frontend.total_categories_app_counts;
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

CREATE UNIQUE INDEX idx_total_categories_app_counts ON frontend.total_categories_app_counts USING btree (
    store, tag_source, app_category
);


DROP MATERIALIZED VIEW IF EXISTS frontend.companies_parent_categories_app_counts;
CREATE MATERIALIZED VIEW frontend.companies_parent_categories_app_counts
TABLESPACE pg_default
AS SELECT
    ad.domain AS company_domain,
    c.name AS company_name,
    csac.app_category,
    count(DISTINCT csac.store_app) AS app_count
FROM adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.companies AS c ON csac.parent_id = c.id
LEFT JOIN adtech.company_domain_mapping AS cdm ON c.id = cdm.company_id
LEFT JOIN ad_domains AS ad ON cdm.domain_id = ad.id
GROUP BY ad.domain, c.name, csac.app_category
ORDER BY c.name, (count(DISTINCT csac.store_app)) DESC
WITH DATA;



-- View indexes:
CREATE UNIQUE INDEX idx_companies_parent_categories_app_counts ON frontend.companies_parent_categories_app_counts USING btree (
    company_domain, company_name, app_category
);


DROP MATERIALIZED VIEW IF EXISTS frontend.companies_sdks_overview;
CREATE MATERIALIZED VIEW frontend.companies_sdks_overview
AS
SELECT
    c.name AS company_name,
    ad.domain AS company_domain,
    parad.domain AS parent_company_domain,
    sdk.sdk_name,
    sp.package_pattern,
    sp2.path_pattern,
    coalesce(
        cc.name,
        c.name
    ) AS parent_company_name
FROM
    adtech.companies AS c
LEFT JOIN adtech.companies AS cc
    ON
        c.parent_company_id = cc.id
LEFT JOIN adtech.company_domain_mapping AS cdm
    ON
        c.id = cdm.company_id
LEFT JOIN adtech.company_domain_mapping AS pcdm
    ON
        cc.id = pcdm.company_id
LEFT JOIN ad_domains AS ad
    ON
        cdm.domain_id = ad.id
LEFT JOIN ad_domains AS parad
    ON
        pcdm.domain_id = parad.id
LEFT JOIN adtech.sdks AS sdk
    ON
        c.id = sdk.company_id
LEFT JOIN adtech.sdk_packages AS sp
    ON
        sdk.id = sp.sdk_id
LEFT JOIN adtech.sdk_paths AS sp2
    ON
        sdk.id = sp2.sdk_id;

CREATE UNIQUE INDEX companies_sdks_overview_unique_idx ON
frontend.companies_sdks_overview (
    company_name,
    company_domain,
    parent_company_domain,
    sdk_name,
    package_pattern,
    path_pattern
);


CREATE MATERIALIZED VIEW frontend.companies_open_source_percent
AS
SELECT
    ad.domain AS company_domain,
    avg(CASE WHEN sd.is_open_source THEN 1 ELSE 0 END) AS percent_open_source
FROM
    adtech.sdks AS sd
LEFT JOIN adtech.company_domain_mapping AS cdm
    ON
        sd.company_id = cdm.company_id
LEFT JOIN ad_domains AS ad
    ON
        cdm.domain_id = ad.id
GROUP BY
    ad.domain;

CREATE UNIQUE INDEX companies_open_source_percent_unique ON
frontend.companies_open_source_percent (
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
        coalesce(
            c.parent_company_id,
            c.id
        ) AS parent_company_id,
        coalesce(
            pc.name,
            c.name
        ) AS parent_company_name
    FROM
        adtech.companies AS c
    LEFT JOIN adtech.companies AS pc ON
        c.parent_company_id = pc.id
)

SELECT DISTINCT
    aav.ad_domain_url,
    myc.parent_company_name AS company_name,
    aav.publisher_id,
    aav.relationship,
    sa.id AS store_app,
    aav.developer_domain_url,
    aav.developer_domain_crawled_at
FROM
    app_ads_view AS aav
LEFT JOIN pub_domains AS pd
    ON
        aav.developer_domain_url::text = pd.url::text
LEFT JOIN app_urls_map AS aum
    ON
        pd.id = aum.pub_domain
LEFT JOIN store_apps AS sa
    ON
        aum.store_app = sa.id
LEFT JOIN adtech.company_domain_mapping AS cdm
    ON
        aav.ad_domain = cdm.domain_id
LEFT JOIN parent_companies AS myc
    ON
        cdm.company_id = myc.company_id
WITH DATA;


-- View indexes:
CREATE INDEX adstxt_entries_store_apps_domain_pub_idx ON frontend.adstxt_entries_store_apps USING btree (
    ad_domain_url, publisher_id
);
CREATE INDEX adstxt_entries_store_apps_idx ON frontend.adstxt_entries_store_apps USING btree (
    store_app
);
CREATE UNIQUE INDEX adstxt_entries_store_apps_unique_idx ON frontend.adstxt_entries_store_apps USING btree (
    ad_domain_url, publisher_id, relationship, store_app
);


-- frontend.adstxt_publishers_overview source
CREATE MATERIALIZED VIEW frontend.adstxt_publishers_overview
TABLESPACE pg_default
AS
WITH ranked_data AS (
    SELECT
        aesa.ad_domain_url,
        aesa.relationship,
        sa.store,
        aesa.publisher_id,
        count(DISTINCT sa.developer) AS developer_count,
        count(DISTINCT aesa.store_app) AS app_count,
        row_number() OVER (
            PARTITION BY
                aesa.ad_domain_url,
                aesa.relationship,
                sa.store
            ORDER BY
                count(DISTINCT aesa.store_app) DESC
        ) AS pubrank
    FROM
        frontend.adstxt_entries_store_apps AS aesa
    LEFT JOIN store_apps AS sa
        ON
            aesa.store_app = sa.id
    GROUP BY
        aesa.ad_domain_url,
        aesa.relationship,
        sa.store,
        aesa.publisher_id
)

SELECT
    rd.ad_domain_url,
    rd.relationship,
    rd.store,
    rd.publisher_id,
    rd.developer_count,
    rd.app_count,
    rd.pubrank
FROM
    ranked_data AS rd
WHERE
    rd.pubrank <= 50
WITH DATA;


-- View indexes:
CREATE INDEX adstxt_publishers_overview_ad_domain_idx ON frontend.adstxt_publishers_overview USING btree (
    ad_domain_url
);
CREATE UNIQUE INDEX adstxt_publishers_overview_ad_domain_unique_idx ON frontend.adstxt_publishers_overview USING btree (
    ad_domain_url, relationship, store, publisher_id
);



-- public.adstxt_ad_domain_overview source

CREATE MATERIALIZED VIEW frontend.adstxt_ad_domain_overview
TABLESPACE pg_default
AS
SELECT
    aesa.ad_domain_url,
    aesa.relationship,
    sa.store,
    count(DISTINCT aesa.publisher_id) AS publisher_id_count,
    count(DISTINCT sa.developer) AS developer_count,
    count(DISTINCT aesa.store_app) AS app_count
FROM
    frontend.adstxt_entries_store_apps AS aesa
LEFT JOIN store_apps AS sa
    ON
        aesa.store_app = sa.id
GROUP BY
    ad_domain_url,
    relationship,
    store
WITH DATA;

-- View indexes:
CREATE INDEX adstxt_ad_domain_overview_idx ON
frontend.adstxt_ad_domain_overview
USING btree (ad_domain_url);
CREATE UNIQUE INDEX adstxt_ad_domain_overview_unique_idx ON
frontend.adstxt_ad_domain_overview
USING btree (
    ad_domain_url,
    relationship,
    store
);

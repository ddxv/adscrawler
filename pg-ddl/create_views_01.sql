CREATE MATERIALIZED VIEW public.app_ads_view
TABLESPACE pg_default
AS SELECT DISTINCT
    pd.url AS developer_domain_url,
    aae.ad_domain,
    aae.publisher_id,
    ad.domain AS ad_domain_url,
    aae.relationship,
    pd.crawl_result,
    pd.crawled_at AS developer_domain_crawled_at,
    ad.updated_at AS ad_domain_updated_at,
    aam.updated_at AS txt_entry_crawled_at
FROM app_ads_entrys AS aae
LEFT JOIN ad_domains AS ad ON aae.ad_domain = ad.id
LEFT JOIN app_ads_map AS aam ON aae.id = aam.app_ads_entry
LEFT JOIN pub_domains AS pd ON aam.pub_domain = pd.id
WHERE pd.crawled_at::date = aam.updated_at::date AND pd.crawl_result = 1
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX app_ads_view_developer_domain_url_idx
ON public.app_ads_view USING btree (
    developer_domain_url, ad_domain, publisher_id, ad_domain_url, relationship
);


-- public.audit_dates source

CREATE MATERIALIZED VIEW public.audit_dates
TABLESPACE pg_default
AS WITH sa AS (
    SELECT
        store_apps_audit.stamp::date AS updated_date,
        'store_apps'::text AS table_name,
        count(*) AS updated_count
    FROM logging.store_apps_audit
    GROUP BY (store_apps_audit.stamp::date)
)

SELECT
    sa.updated_date,
    sa.table_name,
    sa.updated_count
FROM sa
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX audit_dates_updated_date_idx
ON public.audit_dates USING btree (
    updated_date, table_name
);

--for internal dashboard checking last_updated_at and created_ats
--DROP MATERIALIZED VIEW store_apps_updated_at;
CREATE MATERIALIZED VIEW store_apps_updated_at AS
WITH my_dates AS (
    SELECT
        store,
        generate_series(
            current_date - interval '365 days',
            current_date,
            '1 day'::interval
        )::date AS date
    FROM
        generate_series(
            1,
            2,
            1
        ) AS num_series (store)
),

updated_dates AS (
    SELECT
        store,
        updated_at::date AS last_updated_date,
        count(*) AS last_updated_count
    FROM
        store_apps
    WHERE
        updated_at >= current_date - interval '365 days'
    GROUP BY
        store,
        updated_at::date
)

SELECT
    my_dates.store,
    my_dates.date,
    updated_dates.last_updated_count,
    audit_dates.updated_count
FROM
    my_dates
LEFT JOIN updated_dates
    ON
        my_dates.date = updated_dates.last_updated_date
        AND my_dates.store = updated_dates.store
LEFT JOIN audit_dates
    ON
        my_dates.date = audit_dates.updated_date
ORDER BY
    my_dates.date DESC;


CREATE UNIQUE INDEX idx_store_apps_updated_at
ON store_apps_updated_at (store, date);


CREATE MATERIALIZED VIEW store_apps_created_at AS
WITH my_dates AS (
    SELECT
        store,
        generate_series(
            current_date - interval '365 days',
            current_date,
            '1 day'::interval
        )::date AS date
    FROM
        generate_series(
            1,
            2,
            1
        ) AS num_series (store)
),

created_dates AS (
    SELECT
        sa.store,
        sa.created_at::date AS created_date,
        sas.crawl_source,
        count(*) AS created_count
    FROM
        store_apps AS sa
    LEFT JOIN logging.store_app_sources AS sas
        ON
            sa.id = sas.store_app
            AND sa.store = sas.store
    WHERE
        sa.created_at >= current_date - interval '365 days'
    GROUP BY
        sa.store,
        sa.created_at::date,
        sas.crawl_source
)

SELECT
    my_dates.store,
    my_dates.date,
    created_dates.crawl_source,
    created_dates.created_count
FROM
    my_dates
LEFT JOIN created_dates ON
    my_dates.date = created_dates.created_date
    AND my_dates.store = created_dates.store;

CREATE UNIQUE INDEX idx_store_apps_created_at
ON store_apps_created_at (store, date, crawl_source);



-- public.network_counts source

CREATE MATERIALIZED VIEW public.network_counts
TABLESPACE pg_default
AS WITH const AS (
    SELECT count(DISTINCT publisher_network_view.publisher_domain_url) AS count
    FROM publisher_network_view
),

grouped AS (
    SELECT
        publisher_network_view.ad_domain_url,
        publisher_network_view.relationship,
        count(*) AS publishers_count
    FROM publisher_network_view
    GROUP BY
        publisher_network_view.ad_domain_url,
        publisher_network_view.relationship
)

SELECT
    grouped.ad_domain_url,
    grouped.relationship,
    grouped.publishers_count,
    const.count AS publishers_total
FROM grouped
CROSS JOIN const
ORDER BY grouped.publishers_count DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX network_counts_view_idx
ON public.network_counts USING btree (
    ad_domain_url, relationship
);


-- public.network_counts_top source

CREATE MATERIALIZED VIEW public.network_counts_top
TABLESPACE pg_default
AS WITH pubids AS (
    SELECT DISTINCT pdwi.url
    FROM publisher_domain_with_installs AS pdwi
    WHERE
        pdwi.total_installs > 10000000::double precision
        OR pdwi.store = 2 AND pdwi.total_review > 10000::double precision
),

const AS (
    SELECT count(DISTINCT publisher_network_view.publisher_domain_url) AS count
    FROM publisher_network_view
    WHERE (publisher_network_view.publisher_domain_url::text IN (
        SELECT pubids.url
        FROM pubids
    ))
),

grouped AS (
    SELECT
        publisher_network_view.ad_domain_url,
        publisher_network_view.relationship,
        count(*) AS publishers_count
    FROM publisher_network_view
    WHERE (publisher_network_view.publisher_domain_url::text IN (
        SELECT pubids.url
        FROM pubids
    ))
    GROUP BY
        publisher_network_view.ad_domain_url,
        publisher_network_view.relationship
)

SELECT
    grouped.ad_domain_url,
    grouped.relationship,
    grouped.publishers_count,
    const.count AS publishers_total
FROM grouped
CROSS JOIN const
ORDER BY grouped.publishers_count DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX network_counts_top_view_idx
ON public.network_counts_top USING btree (
    ad_domain_url, relationship
);

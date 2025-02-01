CREATE MATERIALIZED VIEW store_apps_overview AS
WITH latest_version_codes AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vc.updated_at,
        vc.crawl_result
    FROM
        version_codes AS vc
    ORDER BY
        vc.store_app,
        string_to_array(vc.version_code, '.')::bigint [] DESC
),

latest_successful_version_codes AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vc.updated_at,
        vc.crawl_result
    FROM
        version_codes AS vc
    WHERE
        vc.crawl_result = 1
    ORDER BY
        vc.store_app,
        vc.version_code::text DESC
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
FROM
    store_apps AS sa
LEFT JOIN developers AS d
    ON
        sa.developer = d.id
LEFT JOIN app_urls_map AS aum
    ON
        sa.id = aum.store_app
LEFT JOIN pub_domains AS pd
    ON
        aum.pub_domain = pd.id
LEFT JOIN latest_version_codes AS lvc
    ON
        sa.id = lvc.store_app
LEFT JOIN latest_successful_version_codes AS lsvc
    ON
        sa.id = lsvc.store_app
WITH DATA;


CREATE INDEX store_apps_overview_idx ON store_apps_overview (store_id);

-- for querying developers' apps
CREATE MATERIALIZED VIEW developer_store_apps
AS
SELECT
    sa.store,
    sa.store_id,
    sa.icon_url_512,
    sa.installs,
    sa.rating,
    sa.rating_count,
    sa.review_count,
    d.name AS developer_name,
    pd.url AS developer_url,
    d.store AS developer_store,
    pd.id AS domain_id,
    d.developer_id
FROM
    store_apps AS sa
LEFT JOIN developers AS d
    ON
        sa.developer = d.id
LEFT JOIN app_urls_map AS aum
    ON
        sa.id = aum.store_app
LEFT JOIN pub_domains AS pd
    ON
        aum.pub_domain = pd.id
WITH DATA;

CREATE INDEX developer_store_apps_idx ON developer_store_apps (developer_id);

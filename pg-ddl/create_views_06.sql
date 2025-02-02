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

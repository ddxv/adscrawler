-- for querying developers' apps
DROP MATERIALIZED VIEW public.developer_store_apps;
CREATE MATERIALIZED VIEW public.developer_store_apps
TABLESPACE pg_default
AS
WITH developer_domain_ids AS (
    SELECT DISTINCT pd_1.id AS domain_id
    FROM app_urls_map AS aum_1
    LEFT JOIN pub_domains AS pd_1 ON aum_1.pub_domain = pd_1.id
    LEFT JOIN store_apps AS sa_1 ON aum_1.store_app = sa_1.id
    LEFT JOIN developers AS d_1 ON sa_1.developer = d_1.id
)

SELECT
    sa.store,
    sa.store_id,
    sa.name,
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
FROM store_apps AS sa
LEFT JOIN developers AS d ON sa.developer = d.id
LEFT JOIN app_urls_map AS aum ON sa.id = aum.store_app
LEFT JOIN pub_domains AS pd ON aum.pub_domain = pd.id
ORDER BY installs DESC NULLS LAST, rating_count DESC NULLS LAST
WITH DATA;


CREATE INDEX developer_store_apps_idx ON developer_store_apps (developer_id);

CREATE INDEX idx_developer_store_apps_domain_id
ON developer_store_apps (domain_id);

CREATE INDEX idx_developer_store_apps_developer_domain
ON developer_store_apps (developer_id, domain_id);

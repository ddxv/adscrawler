CREATE MATERIALIZED VIEW latest_version_codes_mv AS
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
        vc.version_code::TEXT DESC
)

SELECT
    sa.id AS store_app,
    sa.store_id,
    lvc.updated_at AS crawled_at,
    lvc.version_code,
    lvc.crawl_result
FROM
    latest_version_codes AS lvc
LEFT JOIN
    store_apps AS sa
    ON
        lvc.store_app = sa.id;


CREATE INDEX latest_version_codes_mv_idx ON latest_version_codes_mv (store_id);

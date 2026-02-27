WITH candidate_apps AS (
    SELECT
        sa.store,
        sa.id AS store_app,
        sa.ad_supported,
        sa.in_app_purchases,
        sa.category AS app_category
    FROM
        frontend.store_apps_overview AS sa
    WHERE
        (
            sa.crawl_result = 1
            OR sa.store_last_updated >= current_date - INTERVAL '365 days'
        )
        AND sa.installs >= 500
    LIMIT :batch_size
)
SELECT
    agmh.snapshot_date,
    ca.store,
    agmh.store_app,
    ca.app_category,
    ca.ad_supported,
    ca.in_app_purchases,
    agmh.installs,
    agmh.rating_count,
    agmh.review_count,
    agmh.rating,
    agmh.one_star,
    agmh.two_star,
    agmh.three_star,
    agmh.four_star,
    agmh.five_star,
    agmh.tier1_pct,
    agmh.tier2_pct,
    agmh.tier3_pct
FROM candidate_apps AS ca
INNER JOIN app_global_metrics_history AS agmh
    ON ca.store_app = agmh.store_app
WHERE
    agmh.snapshot_date >= current_date - INTERVAL '730 days'
    AND NOT EXISTS (
        SELECT 1
        FROM logging.app_global_metrics_weekly AS lg
        WHERE
            lg.store_app = ca.store_app
            AND lg.updated_at > :start_date
    );

WITH already_processed AS (
    SELECT lg.store_app
    FROM
        logging.app_global_metrics_weekly AS lg
    WHERE
        lg.updated_at > (
            current_date - INTERVAL '3 days'
        )
),
candidate_apps AS (
    SELECT
        sa.store,
        sa.id AS store_app,
        sa.category AS app_category
    FROM
        frontend.store_apps_overview AS sa
    WHERE
        (
            sa.crawl_result = 1
            OR sa.store_last_updated >= current_date - INTERVAL '365 days'
        )
        AND sa.id NOT IN (
            SELECT ap.store_app
            FROM
                already_processed AS ap
        )
    LIMIT :batch_size
)
SELECT
    agmh.snapshot_date,
    ca.store,
    agmh.store_app,
    ca.app_category,
    agmh.installs,
    agmh.rating_count,
    agmh.review_count,
    agmh.rating,
    agmh.one_star,
    agmh.two_star,
    agmh.three_star,
    agmh.four_star,
    agmh.five_star
FROM
    app_global_metrics_history AS agmh
INNER JOIN candidate_apps AS ca
    ON
        agmh.store_app = ca.store_app
WHERE
    agmh.snapshot_date >= current_date - INTERVAL '400 days';

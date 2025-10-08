WITH
my_advs AS (
    SELECT DISTINCT store_app_id
    FROM
        creative_assets AS ca
    LEFT JOIN creative_records AS cr
        ON
            ca.id = cr.creative_asset_id
    LEFT JOIN version_code_api_scan_results AS vcasr
        ON
            cr.run_id = vcasr.id
    WHERE
        vcasr.run_at >= :target_week::date - interval '7 days'
        AND vcasr.run_at < :target_week::date
),
latest_week_per_app AS (
    SELECT
        sahw.store_app,
        MAX(sahw.week_start) AS app_max_week
    FROM
        store_apps_history_weekly AS sahw
    INNER JOIN my_advs AS m
        ON
            m.store_app_id = store_app
    WHERE
        sahw.country_id = 840
    GROUP BY
        sahw.store_app
),
baseline_period AS (
    SELECT
        s.store_app,
        SUM(s.installs_diff) AS baseline_installs_2w,
        SUM(s.rating_count_diff) AS baseline_ratings_2w,
        AVG(s.installs_diff) AS avg_installs_diff,
        STDDEV(s.installs_diff) AS stddev_installs_diff,
        AVG(s.rating_count_diff) AS avg_ratings_diff,
        STDDEV(s.rating_count_diff) AS stddev_ratings_diff
    FROM
        store_apps_history_weekly AS s
    INNER JOIN my_advs AS m
        ON
            s.store_app = m.store_app_id
    WHERE
        s.week_start >= (
            :target_week::date - interval '15 days'
        )
        AND s.week_start <= (
            :target_week::date - interval '1 days'
        )
        AND s.country_id = 840
    GROUP BY
        s.store_app
),
target_weeks_data AS (
    SELECT
        s.store_app,
        :target_week::date AS target_week,
        s.week_start,
        s.installs_diff,
        s.rating_count_diff,
        CASE
            WHEN s.week_start = :target_week::date THEN 1
            ELSE 0
        END AS is_latest_week
    FROM
        store_apps_history_weekly AS s
    INNER JOIN latest_week_per_app AS lwpa
        ON
            s.store_app = lwpa.store_app
    WHERE
        s.week_start >= :target_week::date
        AND
        s.week_start <= (
            :target_week::date + interval '7 days'
        )
        AND s.country_id = 840
),
aggregated_metrics AS (
    SELECT
        rd.store_app,
        rd.target_week,
        SUM(CASE WHEN rd.is_latest_week = 1 THEN rd.installs_diff ELSE 0 END)
            AS installs_sum_1w,
        SUM(
            CASE WHEN rd.is_latest_week = 1 THEN rd.rating_count_diff ELSE 0 END
        ) AS ratings_sum_1w
    FROM
        target_weeks_data AS rd
    GROUP BY
        rd.store_app,
        rd.target_week
),
my_app_z_scores AS (
    SELECT
        am.store_app,
        am.target_week,
        am.installs_sum_1w,
        am.ratings_sum_1w,
        bp.baseline_installs_2w,
        bp.baseline_ratings_2w,
        (am.installs_sum_1w - bp.avg_installs_diff)
        / NULLIF(bp.avg_installs_diff, 0)
        * 100 AS installs_pct_increase,
        (am.ratings_sum_1w - bp.avg_ratings_diff)
        / NULLIF(bp.avg_ratings_diff, 0)
        * 100 AS ratings_pct_increase,
        (
            am.installs_sum_1w - bp.avg_installs_diff
        ) / NULLIF(bp.stddev_installs_diff, 0) AS installs_z_score_1w,
        (
            am.ratings_sum_1w - bp.avg_ratings_diff
        ) / NULLIF(bp.stddev_ratings_diff, 0) AS ratings_z_score_1w
    FROM
        aggregated_metrics AS am
    INNER JOIN baseline_period AS bp ON
        am.store_app = bp.store_app
),
ranked_z_scores AS (
    SELECT
        saz.target_week,
        saz.store_app,
        saz.installs_sum_1w,
        saz.ratings_sum_1w,
        saz.baseline_installs_2w,
        saz.baseline_ratings_2w,
        saz.installs_pct_increase,
        saz.ratings_pct_increase,
        saz.installs_z_score_1w,
        saz.ratings_z_score_1w,
        sa.id,
        sa.developer,
        sa.name,
        sa.store_id,
        sa.store,
        sa.category,
        sa.rating,
        sa.review_count,
        sa.installs,
        sa.free,
        sa.price,
        sa.size,
        sa.minimum_android,
        sa.developer_email,
        sa.store_last_updated,
        sa.content_rating,
        sa.ad_supported,
        sa.in_app_purchases,
        sa.editors_choice,
        sa.created_at,
        sa.updated_at,
        sa.crawl_result,
        sa.icon_url_100,
        sa.icon_url_512,
        sa.release_date,
        sa.rating_count,
        sa.featured_image_url,
        sa.phone_image_url_1,
        sa.phone_image_url_2,
        sa.phone_image_url_3,
        sa.tablet_image_url_1,
        sa.tablet_image_url_2,
        sa.tablet_image_url_3,
        sa.textsearchable_index_col,
        cm.original_category,
        cm.mapped_category,
        ROW_NUMBER() OVER (
            PARTITION BY
                sa.store,
                cm.mapped_category,
                (
                    CASE
                        WHEN sa.store = 2 THEN 'rating'::text
                        ELSE 'installs'::text
                    END
                )
            ORDER BY
                (
                    CASE
                        WHEN sa.store = 2 THEN saz.ratings_z_score_1w
                        WHEN sa.store = 1 THEN saz.installs_z_score_1w
                        ELSE NULL::numeric
                    END
                ) DESC NULLS LAST
        ) AS rn
    FROM
        my_app_z_scores AS saz
    LEFT JOIN store_apps AS sa
        ON
            saz.store_app = sa.id
    LEFT JOIN category_mapping AS cm ON
        sa.category::text = cm.original_category::text
)
SELECT
    target_week,
    store,
    id AS store_app,
    store_id,
    name AS app_name,
    mapped_category AS app_category,
    in_app_purchases,
    ad_supported,
    icon_url_100,
    icon_url_512,
    installs,
    rating_count,
    installs_sum_1w,
    ratings_sum_1w,
    baseline_installs_2w,
    baseline_ratings_2w,
    installs_pct_increase,
    ratings_pct_increase,
    installs_z_score_1w,
    ratings_z_score_1w
FROM
    ranked_z_scores
WHERE
    rn <= 20;

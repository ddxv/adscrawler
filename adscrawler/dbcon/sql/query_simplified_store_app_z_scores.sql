WITH
my_advs AS (
    SELECT DISTINCT cr.advertiser_store_app_id
    FROM
        creative_records AS cr
    LEFT JOIN api_calls AS ac
        ON
            cr.api_call_id = ac.id
    LEFT JOIN version_code_api_scan_results AS vcasr
        ON
            ac.run_id = vcasr.id
    LEFT JOIN adtech.company_domain_mapping AS hcdm
        ON
            cr.creative_host_domain_id = hcdm.domain_id
    LEFT JOIN adtech.companies AS hc
        ON
            hcdm.company_id = hc.id
    LEFT JOIN adtech.company_categories AS hcc
        ON
            hc.id = hcc.company_id
    LEFT JOIN adtech.company_domain_mapping AS icdm
        ON
            cr.creative_initial_domain_id = icdm.domain_id
    LEFT JOIN adtech.companies AS ic
        ON
            icdm.company_id = ic.id
    LEFT JOIN adtech.company_categories AS icc
        ON
            ic.id = icc.company_id
    WHERE
        vcasr.run_at > :target_week::date - interval '7 days'
        AND vcasr.run_at <= :target_week::date
        AND (
            hcc.category_id = 1
        )
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
            s.store_app = m.advertiser_store_app_id
    WHERE
        s.week_start >= (
            :target_week::date - interval '22 days'
        )
        AND s.week_start <= (
            :target_week::date - interval '7 days'
        )
        AND s.country_id = 840
    GROUP BY
        s.store_app
),
target_weeks_data AS (
    SELECT
        s.store_app,
        :target_week::date AS target_week,
        SUM(s.installs_diff) AS target_week_installs,
        SUM(s.rating_count_diff) AS target_week_rating_count
    FROM
        store_apps_history_weekly AS s
    WHERE
        s.week_start >= :target_week::date
        AND
        s.week_start < (
            :target_week::date + interval '7 days'
        )
        AND s.country_id = 840
    GROUP BY
        s.store_app,
        target_week
),
my_app_z_scores AS (
    SELECT
        tw.store_app,
        tw.target_week,
        tw.target_week_installs,
        tw.target_week_rating_count,
        bp.baseline_installs_2w,
        bp.baseline_ratings_2w,
        (
            tw.target_week_installs - bp.avg_installs_diff
        )
        / NULLIF(bp.avg_installs_diff, 0)
        * 100 AS installs_pct_increase,
        (
            tw.target_week_rating_count - bp.avg_ratings_diff
        )
        / NULLIF(bp.avg_ratings_diff, 0)
        * 100 AS ratings_pct_increase,
        (
            tw.target_week_installs - bp.avg_installs_diff
        ) / NULLIF(bp.stddev_installs_diff, 0) AS installs_z_score_1w,
        (
            tw.target_week_rating_count - bp.avg_ratings_diff
        ) / NULLIF(bp.stddev_ratings_diff, 0) AS ratings_z_score_1w
    FROM
        target_weeks_data AS tw
    INNER JOIN baseline_period AS bp ON
        tw.store_app = bp.store_app
),
ranked_z_scores AS (
    SELECT
        saz.target_week,
        saz.store_app,
        saz.target_week_installs,
        saz.target_week_rating_count,
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
        agm.installs,
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
        ROW_NUMBER() OVER (
            PARTITION BY
                sa.store,
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
      LEFT JOIN app_global_metrics_latest AS agm
        ON sa.id = agm.store_app
    LEFT JOIN category_mapping AS cm ON
        sa.category::text = cm.original_category::text
)
SELECT
    target_week,
    store,
    id AS store_app,
    store_id,
    name AS app_name,
    in_app_purchases,
    ad_supported,
    icon_url_100,
    installs,
    rating_count,
    target_week_installs,
    target_week_rating_count,
    baseline_installs_2w,
    baseline_ratings_2w,
    installs_pct_increase,
    ratings_pct_increase,
    installs_z_score_1w,
    ratings_z_score_1w
FROM
    ranked_z_scores
WHERE
    rn <= 50;

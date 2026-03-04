WITH my_advs AS (
    SELECT DISTINCT cr.advertiser_store_app_id
    FROM creative_records AS cr
    LEFT JOIN api_calls AS ac ON cr.api_call_id = ac.id
    LEFT JOIN version_code_api_scan_results AS vcasr ON ac.run_id = vcasr.id
    LEFT JOIN
        adtech.company_domain_mapping AS hcdm
        ON cr.creative_host_domain_id = hcdm.domain_id
    LEFT JOIN adtech.companies AS hc ON hcdm.company_id = hc.id
    LEFT JOIN adtech.company_categories AS hcc ON hc.id = hcc.company_id
    WHERE
        vcasr.run_at > CAST(:target_week AS date) - interval '7 days'
        AND vcasr.run_at <= CAST(:target_week AS date)
        AND hcc.category_id = 1 AND cr.advertiser_store_app_id IS NOT NULL
),
windowed_metrics AS (
    SELECT
        ga.store_app,
        ga.week_start,
        ga.weekly_installs,
        ga.weekly_ratings,
        CAST(:target_week AS date) AS target_week,
        -- Current week performance
        ga.weekly_installs AS target_week_installs,
        -- 2nd Week Average (Current + 1 Prior)
        AVG(ga.weekly_installs) OVER w_2w AS installs_avg_2w,
        -- 4th Week Average (Current + 3 Prior)
        AVG(ga.weekly_installs) OVER w_4w AS installs_avg_4w,
        -- Baseline: 8-week window ending 4 weeks prior to target_week
        AVG(
            CASE
                WHEN
                    ga.week_start
                    >= CAST(:target_week AS date) - interval '112 days'
                    AND ga.week_start
                    <= CAST(:target_week AS date) - interval '28 days'
                    THEN ga.weekly_installs
            END
        ) OVER w_all AS b_avg_installs,
        STDDEV(
            CASE
                WHEN
                    ga.week_start
                    >= CAST(:target_week AS date) - interval '112 days'
                    AND ga.week_start
                    <= CAST(:target_week AS date) - interval '28 days'
                    THEN ga.weekly_installs
            END
        ) OVER w_all AS b_std_installs
    FROM app_global_metrics_weekly AS ga
    INNER JOIN my_advs AS m ON ga.store_app = m.advertiser_store_app_id
    -- Lookback range to populate the windows
    WHERE
        ga.week_start >= (CAST(:target_week AS date) - interval '130 days')
        AND ga.week_start <= CAST(:target_week AS date)
    WINDOW
        w_all AS (PARTITION BY ga.store_app),
        w_4w AS (
            PARTITION BY ga.store_app
            ORDER BY ga.week_start DESC ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ),
        w_2w AS (
            PARTITION BY ga.store_app
            ORDER BY ga.week_start DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
        )
),
final_calculation AS (
    SELECT
        wm.*,
        sa.store,
        sa.name AS app_name,
        sa.store_id,
        sa.icon_url_100,
        sa.ad_supported,
        sa.in_app_purchases,
        -- Calculations for the three requested columns
        b_avg_installs AS baseline_installs,
        ((installs_avg_2w - b_avg_installs) / NULLIF(b_avg_installs, 0))
        * 100 AS baseline_installs_pct,
        ((weekly_installs - b_avg_installs) / NULLIF(b_avg_installs, 0))
        * 100 AS weekly_installs_pct,
        -- Z-Scores and Acceleration
        (installs_avg_2w - b_avg_installs)
        / NULLIF(b_std_installs, 0::numeric) AS installs_z_score_2w,
        (installs_avg_4w - b_avg_installs)
        / NULLIF(b_std_installs, 0::numeric) AS installs_z_score_4w,
        (installs_avg_2w - installs_avg_4w)
        / NULLIF(installs_avg_4w, 0::numeric) AS installs_acceleration,
        b_std_installs IS NOT NULL
        AND b_avg_installs > 0 AS has_reliable_baseline,
        ROW_NUMBER() OVER (
            PARTITION BY sa.store
            ORDER BY
                (installs_avg_2w - b_avg_installs)
                / NULLIF(b_std_installs, 0::numeric) DESC NULLS LAST
        ) AS rn
    FROM windowed_metrics AS wm
    INNER JOIN store_apps AS sa ON wm.store_app = sa.id
    WHERE wm.week_start = wm.target_week
)
SELECT
    target_week,
    store,
    store_app,
    store_id,
    app_name,
    in_app_purchases,
    ad_supported,
    icon_url_100,
    target_week_installs,
    baseline_installs,
    baseline_installs_pct,
    weekly_installs_pct,
    installs_z_score_2w,
    installs_z_score_4w,
    installs_acceleration,
    has_reliable_baseline
FROM final_calculation
WHERE rn <= 50;

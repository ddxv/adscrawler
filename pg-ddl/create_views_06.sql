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


-- public.store_app_z_scores source

CREATE MATERIALIZED VIEW public.store_app_z_scores
TABLESPACE pg_default
AS WITH latest_week AS (
    SELECT max(store_apps_history_weekly.week_start) AS max_week
    FROM store_apps_history_weekly
),

latest_week_per_app AS (
    SELECT
        store_apps_history_weekly.store_app,
        max(store_apps_history_weekly.week_start) AS app_max_week
    FROM store_apps_history_weekly
    WHERE store_apps_history_weekly.country_id = 840
    GROUP BY store_apps_history_weekly.store_app
),

baseline_period AS (
    SELECT
        store_apps_history_weekly.store_app,
        avg(store_apps_history_weekly.installs_diff) AS avg_installs_diff,
        stddev(store_apps_history_weekly.installs_diff) AS stddev_installs_diff,
        avg(store_apps_history_weekly.rating_count_diff) AS avg_rating_diff,
        stddev(
            store_apps_history_weekly.rating_count_diff
        ) AS stddev_rating_diff
    FROM store_apps_history_weekly,
        latest_week
    WHERE
        store_apps_history_weekly.week_start
        >= (latest_week.max_week - '84 days'::interval)
        AND store_apps_history_weekly.week_start
        <= (latest_week.max_week - '35 days'::interval)
        AND store_apps_history_weekly.country_id = 840
    GROUP BY store_apps_history_weekly.store_app
),

recent_data AS (
    SELECT
        s.store_app,
        lw.max_week,
        s.week_start,
        s.installs_diff,
        s.rating_count_diff,
        CASE
            WHEN s.week_start = lwpa.app_max_week THEN 1
            ELSE 0
        END AS is_latest_week,
        CASE
            WHEN
                s.week_start >= (lwpa.app_max_week - '14 days'::interval)
                THEN 1
            ELSE 0
        END AS in_2w_period,
        CASE
            WHEN
                s.week_start >= (lwpa.app_max_week - '28 days'::interval)
                THEN 1
            ELSE 0
        END AS in_4w_period
    FROM store_apps_history_weekly AS s
    CROSS JOIN latest_week AS lw
    INNER JOIN latest_week_per_app AS lwpa ON s.store_app = lwpa.store_app
    WHERE
        s.week_start >= (lw.max_week - '28 days'::interval)
        AND s.country_id = 840
),

aggregated_metrics AS (
    SELECT
        rd.store_app,
        rd.max_week AS latest_week,
        sum(
            CASE
                WHEN rd.is_latest_week = 1 THEN rd.installs_diff
                ELSE 0::numeric
            END
        ) AS installs_sum_1w,
        sum(
            CASE
                WHEN rd.is_latest_week = 1 THEN rd.rating_count_diff
                ELSE 0::bigint
            END
        ) AS ratings_sum_1w,
        sum(
            CASE
                WHEN rd.in_2w_period = 1 THEN rd.installs_diff
                ELSE 0::numeric
            END) / nullif(sum(
            CASE
                WHEN rd.in_2w_period = 1 THEN 1
                ELSE 0
            END
        ), 0)::numeric AS installs_avg_2w,
        sum(
            CASE
                WHEN rd.in_2w_period = 1 THEN rd.rating_count_diff
                ELSE 0::bigint
            END) / nullif(sum(
            CASE
                WHEN rd.in_2w_period = 1 THEN 1
                ELSE 0
            END
        ), 0)::numeric AS ratings_avg_2w,
        sum(
            CASE
                WHEN rd.in_4w_period = 1 THEN rd.installs_diff
                ELSE 0::numeric
            END
        ) AS installs_sum_4w,
        sum(
            CASE
                WHEN rd.in_4w_period = 1 THEN rd.rating_count_diff
                ELSE 0::bigint
            END
        ) AS ratings_sum_4w,
        sum(
            CASE
                WHEN rd.in_4w_period = 1 THEN rd.installs_diff
                ELSE 0::numeric
            END) / nullif(sum(
            CASE
                WHEN rd.in_4w_period = 1 THEN 1
                ELSE 0
            END
        ), 0)::numeric AS installs_avg_4w,
        sum(
            CASE
                WHEN rd.in_4w_period = 1 THEN rd.rating_count_diff
                ELSE 0::bigint
            END) / nullif(sum(
            CASE
                WHEN rd.in_4w_period = 1 THEN 1
                ELSE 0
            END
        ), 0)::numeric AS ratings_avg_4w
    FROM recent_data AS rd
    GROUP BY rd.store_app, rd.max_week
)

SELECT
    am.store_app,
    am.latest_week,
    am.installs_sum_1w,
    am.ratings_sum_1w,
    am.installs_avg_2w,
    am.ratings_avg_2w,
    am.installs_sum_4w,
    am.ratings_sum_4w,
    am.installs_avg_4w,
    am.ratings_avg_4w,
    (am.installs_avg_2w - bp.avg_installs_diff)
    / nullif(bp.stddev_installs_diff, 0::numeric) AS installs_z_score_2w,
    (am.ratings_avg_2w - bp.avg_rating_diff)
    / nullif(bp.stddev_rating_diff, 0::numeric) AS ratings_z_score_2w,
    (am.installs_avg_4w - bp.avg_installs_diff)
    / nullif(bp.stddev_installs_diff, 0::numeric) AS installs_z_score_4w,
    (am.ratings_avg_4w - bp.avg_rating_diff)
    / nullif(bp.stddev_rating_diff, 0::numeric) AS ratings_z_score_4w
FROM aggregated_metrics AS am
INNER JOIN baseline_period AS bp ON am.store_app = bp.store_app
WITH DATA;


-- frontend.store_apps_z_scores source

CREATE MATERIALIZED VIEW frontend.store_apps_z_scores
TABLESPACE pg_default
AS WITH ranked_z_scores AS (
    SELECT
        saz.store_app,
        saz.latest_week,
        saz.installs_sum_1w,
        saz.ratings_sum_1w,
        saz.installs_avg_2w,
        saz.ratings_avg_2w,
        saz.installs_z_score_2w,
        saz.ratings_z_score_2w,
        saz.installs_sum_4w,
        saz.ratings_sum_4w,
        saz.installs_avg_4w,
        saz.ratings_avg_4w,
        saz.installs_z_score_4w,
        saz.ratings_z_score_4w,
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
        row_number() OVER (PARTITION BY
            sa.store, cm.mapped_category, (
                CASE
                    WHEN sa.store = 2 THEN 'rating'::text
                    ELSE 'installs'::text
                END
            )
        ORDER BY (
            CASE
                WHEN sa.store = 2 THEN saz.ratings_z_score_2w
                WHEN sa.store = 1 THEN saz.installs_z_score_2w
                ELSE NULL::numeric
            END
        ) DESC NULLS LAST) AS rn
    FROM store_app_z_scores AS saz
    LEFT JOIN store_apps AS sa ON saz.store_app = sa.id
    LEFT JOIN
        category_mapping AS cm
        ON sa.category::text = cm.original_category::text
)

SELECT
    store,
    store_id,
    name AS app_name,
    mapped_category AS app_category,
    in_app_purchases,
    ad_supported,
    icon_url_512,
    installs,
    rating_count,
    installs_sum_1w,
    ratings_sum_1w,
    installs_avg_2w,
    ratings_avg_2w,
    installs_z_score_2w,
    ratings_z_score_2w,
    installs_sum_4w,
    ratings_sum_4w,
    installs_avg_4w,
    ratings_avg_4w,
    installs_z_score_4w,
    ratings_z_score_4w
FROM ranked_z_scores
WHERE rn <= 100
WITH DATA;

CREATE UNIQUE INDEX frontend_store_apps_z_scores_unique ON frontend.store_apps_z_scores USING btree (
    store,
    store_id
);


-- frontend.creative_rankings source
CREATE MATERIALIZED VIEW frontend.creative_rankings
TABLESPACE pg_default
AS
WITH creative_counts AS (
    SELECT
        ca.store_app_id AS advertiser_store_app_id,
        ca.file_extension,
        coalesce(ca.phash, ca.md5_hash) AS vhash,
        count(DISTINCT cr.store_app_pub_id) AS distinct_pub_count,
        min(md5_hash) AS md5_hash
    FROM creative_assets AS ca
    INNER JOIN creative_records AS cr ON ca.id = cr.creative_asset_id
    GROUP BY ca.store_app_id, coalesce(ca.phash, ca.md5_hash), ca.file_extension
),

creative_rankings AS (
    SELECT
        cc.advertiser_store_app_id,
        cc.vhash,
        cc.md5_hash,
        cc.file_extension,
        cc.distinct_pub_count,
        row_number() OVER (
            PARTITION BY cc.advertiser_store_app_id
            ORDER BY cc.distinct_pub_count DESC
        ) AS rn
    FROM creative_counts AS cc
)

SELECT
    saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_512 AS advertiser_icon_url_512,
    saa.category AS advertiser_category,
    saa.installs AS advertiser_installs,
    count(DISTINCT ca.phash) AS unique_creatives,
    count(DISTINCT cr.store_app_pub_id) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    avg(sap.installs) AS avg_publisher_installs,
    array(
        SELECT crk.md5_hash
        FROM creative_rankings AS crk
        WHERE crk.advertiser_store_app_id = saa.id AND crk.rn <= 4
        ORDER BY crk.rn
    ) AS top_md5_hashes
FROM creative_records AS cr
LEFT JOIN creative_assets AS ca ON cr.creative_asset_id = ca.id
LEFT JOIN frontend.store_apps_overview AS sap ON cr.store_app_pub_id = sap.id
LEFT JOIN frontend.store_apps_overview AS saa ON ca.store_app_id = saa.id
LEFT JOIN version_code_api_scan_results AS vcasr ON cr.run_id = vcasr.id
GROUP BY
    saa.name, saa.store_id, saa.icon_url_512, saa.category, saa.installs, saa.id
ORDER BY (count(DISTINCT ca.md5_hash)) DESC
WITH DATA;

-- frontend.advertiser_creatives source
CREATE MATERIALIZED VIEW frontend.advertiser_creatives
TABLESPACE pg_default
AS SELECT
    saa.store_id AS advertiser_store_id,
    cr.run_id,
    vcasr.run_at,
    sap.name AS pub_name,
    sap.store_id AS pub_store_id,
    hd.domain AS host_domain,
    hcd.domain AS host_domain_company_domain,
    hc.name AS host_domain_company_name,
    ad.domain AS ad_domain,
    acd.domain AS ad_domain_company_domain,
    ac.name AS ad_domain_company_name,
    ca.md5_hash,
    ca.file_extension,
    sap.icon_url_512 AS pub_icon_url_512,
    coalesce(ca.phash, ca.md5_hash) AS vhash
FROM creative_records AS cr
LEFT JOIN creative_assets AS ca ON cr.creative_asset_id = ca.id
LEFT JOIN frontend.store_apps_overview AS sap ON cr.store_app_pub_id = sap.id
LEFT JOIN frontend.store_apps_overview AS saa ON ca.store_app_id = saa.id
LEFT JOIN version_code_api_scan_results AS vcasr ON cr.run_id = vcasr.id
LEFT JOIN ad_domains AS hd ON cr.creative_host_domain_id = hd.id
LEFT JOIN ad_domains AS ad ON cr.creative_initial_domain_id = ad.id
LEFT JOIN adtech.company_domain_mapping AS hcdm ON hd.id = hcdm.domain_id
LEFT JOIN adtech.company_domain_mapping AS acdm ON hd.id = acdm.domain_id
LEFT JOIN adtech.companies AS hc ON hcdm.company_id = hc.id
LEFT JOIN adtech.companies AS ac ON acdm.company_id = ac.id
LEFT JOIN ad_domains AS hcd ON hc.domain_id = hcd.id
LEFT JOIN ad_domains AS acd ON ac.domain_id = acd.id
WITH DATA;

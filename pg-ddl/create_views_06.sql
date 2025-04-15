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



CREATE MATERIALIZED VIEW store_apps_z_scores AS
WITH latest_week AS (
    SELECT MAX(week_start) - INTERVAL '1 week' AS max_week
    FROM
        store_apps_history_weekly
),

combined_stats AS (
    SELECT
        s.store_app,
        s.week_start,
        s.installs_diff,
        s.rating_count_diff,
        lw.max_week AS latest_week,
        -- Base period statistics
        AVG(s.installs_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week
            - INTERVAL '12 weeks' AND lw.max_week
            - INTERVAL '5 weeks'
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS avg_installs_diff,
        STDDEV(s.installs_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week
            - INTERVAL '12 weeks' AND lw.max_week
            - INTERVAL '5 weeks'
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS stddev_installs_diff,
        AVG(s.rating_count_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week
            - INTERVAL '12 weeks' AND lw.max_week
            - INTERVAL '5 weeks'
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS avg_rating_diff,
        STDDEV(s.rating_count_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week
            - INTERVAL '12 weeks' AND lw.max_week
            - INTERVAL '5 weeks'
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS stddev_rating_diff,
        -- avg 2-week
        AVG(s.installs_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week - INTERVAL '1 week' AND lw.max_week
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS avg_2week_installs,
        AVG(s.rating_count_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week - INTERVAL '1 week' AND lw.max_week
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS avg_2week_ratings,
        -- avg 4-week
        AVG(s.installs_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week
            - INTERVAL '3 weeks' AND lw.max_week
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS avg_4week_installs,
        AVG(s.rating_count_diff) FILTER (
            WHERE
            s.week_start BETWEEN lw.max_week
            - INTERVAL '3 weeks' AND lw.max_week
        )
            OVER (
                PARTITION BY s.store_app
            )
        AS avg_4week_ratings
    FROM
        store_apps_history_weekly AS s
    CROSS JOIN latest_week AS lw
    WHERE
        s.week_start >= lw.max_week - INTERVAL '12 weeks'
        AND s.country_id = 840
)

SELECT
    store_app,
    latest_week,
    avg_2week_installs,
    avg_2week_ratings,
    avg_4week_installs,
    avg_4week_ratings,
    (
        avg_2week_installs - avg_installs_diff
    ) / NULLIF(stddev_installs_diff, 0) AS installs_z_score_2w,
    (
        avg_2week_ratings - avg_rating_diff
    ) / NULLIF(stddev_rating_diff, 0) AS rating_z_score_2w,
    (
        avg_4week_installs - avg_installs_diff
    ) / NULLIF(stddev_installs_diff, 0) AS installs_z_score_4w,
    (
        avg_4week_ratings - avg_rating_diff
    ) / NULLIF(stddev_rating_diff, 0) AS rating_z_score_4w
FROM
    combined_stats
WHERE
    week_start = latest_week;

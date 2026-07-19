COPY (
WITH
pre_agg AS (
    SELECT
        year,
        quarter,
        store,
        tag_source,
        COUNT(DISTINCT store_app) AS total_apps_in_quarter
    FROM enriched_windowed e
    GROUP BY
        year,
        quarter,
        store,
        tag_source
),

current_quarter AS (
    SELECT
        e.domain_id,
        e.year,
        e.quarter,
        e.store,
        e.tag_source,
        COUNT(*) AS total_apps,
        p.total_apps_in_quarter
    FROM enriched_windowed e
    JOIN pre_agg p
      ON p.year = e.year
     AND p.quarter = e.quarter
     AND p.store = e.store
     AND p.tag_source = e.tag_source
    GROUP BY
        e.domain_id,
        e.year,
        e.quarter,
        e.store,
        e.tag_source,
        p.total_apps_in_quarter
),


churned AS (
    SELECT
        domain_id,
        store,
        tag_source,
        CASE WHEN quarter = 4 THEN year + 1 ELSE year END AS year,
        CASE WHEN quarter = 4 THEN 1 ELSE quarter + 1 END AS quarter,
        COUNT(*) AS apps_lost
    FROM enriched_windowed
    WHERE
        next_yq IS NULL
        OR next_yq != yq + next_delta
    GROUP BY
        domain_id,
        store,
        tag_source,
        year,
        quarter
),

added AS (
    SELECT
        domain_id,
        store,
        tag_source,
        year,
        quarter,
        COUNT(*) AS apps_added
    FROM enriched_windowed
    WHERE
        prev_yq IS NULL
        OR prev_yq != yq - prev_delta
    GROUP BY
        domain_id,
        store,
        tag_source,
        year,
        quarter
)

SELECT
    d.domain_name,
    cq.year,
    cq.quarter,
    cq.store,
    cq.tag_source,
    cq.total_apps,
    cq.total_apps_in_quarter,
    COALESCE(ch.apps_lost, 0) AS apps_lost,
    COALESCE(a.apps_added, 0) AS apps_added,
    ROUND(
        cq.total_apps * 100.0
        / NULLIF(cq.total_apps_in_quarter, 0),
        5
    ) AS pct_market_share,
    ROUND(
        COALESCE(a.apps_added, 0) * 100.0
        / NULLIF(cq.total_apps - COALESCE(a.apps_added, 0), 0),
        2
    ) AS pct_apps_added,
    ROUND(
        COALESCE(ch.apps_lost, 0) * 100.0
        / NULLIF(cq.total_apps + COALESCE(ch.apps_lost, 0), 0),
        2
    ) AS pct_apps_lost
FROM current_quarter cq
LEFT JOIN churned ch
    ON ch.domain_id = cq.domain_id
   AND ch.year = cq.year
   AND ch.quarter = cq.quarter
   AND ch.store = cq.store
   AND ch.tag_source = cq.tag_source
LEFT JOIN added a
    ON a.domain_id = cq.domain_id
   AND a.year = cq.year
   AND a.quarter = cq.quarter
   AND a.store = cq.store
   AND a.tag_source = cq.tag_source
LEFT JOIN pg.domains d
    ON d.id = cq.domain_id
ORDER BY
    cq.year,
    cq.quarter,
    cq.tag_source,
    cq.total_apps DESC
)
TO '/tmp/trend_domains.parquet'
(
    FORMAT PARQUET,
    ROW_GROUP_SIZE 100000,
    COMPRESSION 'zstd',
    OVERWRITE_OR_IGNORE true
);
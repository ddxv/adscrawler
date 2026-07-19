CREATE TEMP TABLE enriched_company AS
SELECT 
    cdm.company_id,
    e.store_app,
    e.year,
    e.quarter,
    e.tag_source
FROM enriched e
JOIN company_domain_mapping_cache cdm 
  ON e.domain_id = cdm.domain_id
GROUP BY cdm.company_id, e.store_app, e.year, e.quarter, e.tag_source;

CREATE TEMP TABLE enriched_windowed_company AS
SELECT
    ec.*,
    sa.store,
    (ec.year * 10 + ec.quarter)                                 AS yq,
    CASE WHEN ec.quarter = 1 THEN 7 ELSE 1 END                  AS prev_delta,
    CASE WHEN ec.quarter = 4 THEN 7 ELSE 1 END                  AS next_delta,
    LAG(ec.year * 10 + ec.quarter)  OVER w                      AS prev_yq,
    LEAD(ec.year * 10 + ec.quarter) OVER w                      AS next_yq
FROM enriched_company ec
LEFT JOIN store_app_store sa
  ON sa.id = ec.store_app
WINDOW w AS (PARTITION BY ec.company_id, ec.store_app, ec.tag_source ORDER BY ec.year, ec.quarter);

COPY (
WITH
pre_agg AS (
    SELECT
        year,
        quarter,
        store,
        tag_source,
        COUNT(DISTINCT store_app) AS total_apps_in_quarter
    -- Uses company table, though distinct app count across the market remains identical
    FROM enriched_windowed_company 
    GROUP BY year, quarter, store, tag_source
),

current_quarter AS (
    SELECT
        e.company_id,
        e.year,
        e.quarter,
        e.store,
        e.tag_source,
        COUNT(*) AS total_apps,
        p.total_apps_in_quarter
    FROM enriched_windowed_company e
    JOIN pre_agg p
      ON p.year = e.year
     AND p.quarter = e.quarter
     AND p.store = e.store
     AND p.tag_source = e.tag_source
    GROUP BY
        e.company_id,
        e.year,
        e.quarter,
        e.store,
        e.tag_source,
        p.total_apps_in_quarter
),

churned AS (
    SELECT
        company_id,
        store,
        tag_source,
        CASE WHEN quarter = 4 THEN year + 1 ELSE year END AS year,
        CASE WHEN quarter = 4 THEN 1 ELSE quarter + 1 END AS quarter,
        COUNT(*) AS apps_lost
    FROM enriched_windowed_company
    WHERE next_yq IS NULL OR next_yq != yq + next_delta
    GROUP BY company_id, store, tag_source, year, quarter
),

added AS (
    SELECT
        company_id,
        store,
        tag_source,
        year,
        quarter,
        COUNT(*) AS apps_added
    FROM enriched_windowed_company
    WHERE prev_yq IS NULL OR prev_yq != yq - prev_delta
    GROUP BY company_id, store, tag_source, year, quarter
)

SELECT
    ad.domain_name AS company_domain,
    cq.year,
    cq.quarter,
    cq.store,
    cq.tag_source,
    cq.total_apps,
    cq.total_apps_in_quarter,
    COALESCE(ch.apps_lost, 0) AS apps_lost,
    COALESCE(a.apps_added, 0) AS apps_added,
    ROUND(
        cq.total_apps * 100.0 / NULLIF(cq.total_apps_in_quarter, 0), 
        5
    ) AS pct_market_share,
    ROUND(
        COALESCE(a.apps_added, 0) * 100.0 / NULLIF(cq.total_apps - COALESCE(a.apps_added, 0), 0), 
        2
    ) AS pct_apps_added,
    ROUND(
        COALESCE(ch.apps_lost, 0) * 100.0 / NULLIF(cq.total_apps + COALESCE(ch.apps_lost, 0), 0), 
        2
    ) AS pct_apps_lost
FROM current_quarter cq
LEFT JOIN churned ch
    ON ch.company_id = cq.company_id
   AND ch.year = cq.year
   AND ch.quarter = cq.quarter
   AND ch.store = cq.store
   AND ch.tag_source = cq.tag_source
LEFT JOIN added a
    ON a.company_id = cq.company_id
   AND a.year = cq.year
   AND a.quarter = cq.quarter
   AND a.store = cq.store
   AND a.tag_source = cq.tag_source
-- Map company back to metadata using your pre-cached items
LEFT JOIN companies_cache co 
    ON cq.company_id = co.id
LEFT JOIN domains_cache ad 
    ON co.domain_id = ad.id
ORDER BY
    cq.year,
    cq.quarter,
    cq.tag_source,
    cq.total_apps DESC
)
TO '/tmp/trend_companies.parquet'
(
    FORMAT PARQUET,
    ROW_GROUP_SIZE 100000,
    COMPRESSION 'zstd',
    OVERWRITE_OR_IGNORE true
);
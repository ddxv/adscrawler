COPY (
    WITH enriched_raw AS (
    SELECT
        h.domain_id,
        h.store_app,
        h.sdk,
        h.api_call,
        h.app_ads_direct,
        h.year,
        h.quarter,
        sa.release_date
    FROM :parquet_files
    LEFT JOIN store_apps sa ON sa.id = h.store_app
),
enriched AS (
    SELECT domain_id, store_app, year, quarter, release_date, 'sdk' AS tag_source
    FROM enriched_raw WHERE sdk
    UNION ALL
    SELECT domain_id, store_app, year, quarter, release_date, 'api_call' AS tag_source
    FROM enriched_raw WHERE api_call
    UNION ALL
    SELECT domain_id, store_app, year, quarter, release_date, 'app_ads_direct' AS tag_source
    FROM enriched_raw WHERE app_ads_direct
),
max_period AS (
    SELECT max(year * 10 + quarter) AS max_yq FROM enriched
),
app_tag_first_seen AS (
    SELECT store_app, tag_source, min(year * 10 + quarter) AS first_seen_key
    FROM enriched
    GROUP BY store_app, tag_source
),
added AS (
    SELECT
        c.domain_id,
        c.store_app,
        c.tag_source,
        c.year,
        c.quarter,
        CASE
            WHEN (c.year * 10 + c.quarter) = af.first_seen_key
             AND NOT (
                c.release_date >= make_date(CAST(c.year AS INTEGER), (c.quarter - 1) * 3 + 1, 1)
                AND c.release_date < make_date(CAST(c.year AS INTEGER), (c.quarter - 1) * 3 + 1, 1) + INTERVAL '3 months'
             )
            THEN 'added_initial'
            ELSE 'added'
        END AS status
    FROM enriched c
    LEFT JOIN enriched p
        ON c.domain_id = p.domain_id
       AND c.store_app = p.store_app
       AND c.tag_source = p.tag_source
       AND (
            (c.quarter = 1 AND p.year = c.year - 1 AND p.quarter = 4)
            OR (c.quarter > 1 AND p.year = c.year AND p.quarter = c.quarter - 1)
       )
    JOIN app_tag_first_seen af
        ON c.store_app = af.store_app AND c.tag_source = af.tag_source
    WHERE p.store_app IS NULL
),
removed AS (
    SELECT
        p.domain_id,
        p.store_app,
        p.tag_source,
        CASE WHEN p.quarter = 4 THEN p.year + 1 ELSE CAST(p.year AS INTEGER) END AS year,
        CASE WHEN p.quarter = 4 THEN 1 ELSE p.quarter + 1 END AS quarter,
        'removed' AS status
    FROM enriched p
    CROSS JOIN max_period mp
    LEFT JOIN enriched c
        ON p.domain_id = c.domain_id
       AND p.store_app = c.store_app
       AND p.tag_source = c.tag_source
       AND (
            (p.quarter = 4 AND c.year = p.year + 1 AND c.quarter = 1)
            OR (p.quarter < 4 AND c.year = p.year AND c.quarter = p.quarter + 1)
       )
    WHERE c.store_app IS NULL
      AND (
          (CASE WHEN p.quarter = 4 THEN p.year + 1 ELSE CAST(p.year AS INTEGER) END) * 10
          + (CASE WHEN p.quarter = 4 THEN 1 ELSE p.quarter + 1 END)
      ) <= mp.max_yq
)
SELECT domain_id, store_app, tag_source, year, quarter, status FROM added
UNION ALL
SELECT domain_id, store_app, tag_source, year, quarter, status FROM removed
) TO '/tmp/combined_domain_app_history.parquet'
          (
              FORMAT PARQUET,
              ROW_GROUP_SIZE 100000,
              COMPRESSION 'zstd',
              OVERWRITE_OR_IGNORE true
          )
;
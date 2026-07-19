CREATE TEMP TABLE enriched AS
SELECT domain_id, store_app, year, quarter, tag_source
FROM (
    SELECT
        h.domain_id,
        h.store_app,
        h.year,
        h.quarter,
        h.sdk,
        h.api_call,
        h.app_ads_direct
    FROM read_parquet($parquet_files) h
)
UNPIVOT (is_active FOR tag_source IN (sdk, api_call, app_ads_direct))
WHERE is_active = true;

CREATE TEMP TABLE enriched_windowed AS
SELECT
    e.*,
    sa.store,
    sa.release_date,
    make_date(
        CAST(year AS INTEGER),
        (quarter - 1) * 3 + 1,
        1
    ) AS quarter_start,
    (year * 10 + quarter)                                       AS yq,
    CASE WHEN quarter = 1 THEN 7 ELSE 1 END                     AS prev_delta,  -- yq gap Q1→prevQ4
    CASE WHEN quarter = 4 THEN 7 ELSE 1 END                     AS next_delta,  -- yq gap Q4→nextQ1
    LAG(year * 10 + quarter)  OVER w                            AS prev_yq,
    LEAD(year * 10 + quarter) OVER w                            AS next_yq,
    MIN(year * 10 + quarter) OVER (PARTITION BY store_app, tag_source) AS first_seen_key,
    MAX(year * 10 + quarter) OVER ()                                   AS max_yq
FROM enriched e
LEFT JOIN store_app_store sa
  ON sa.id = e.store_app
WINDOW w AS (PARTITION BY domain_id, store_app, tag_source ORDER BY year, quarter);

COPY (
    WITH added AS (
        SELECT
            w.domain_id,
            w.store_app,
            w.tag_source,
            w.year,
            w.quarter,
            CASE
                WHEN w.yq = w.first_seen_key
                 AND NOT (
                        w.release_date >= w.quarter_start
                    AND w.release_date <  w.quarter_start + INTERVAL '3 months'
                 )
                THEN 'added_initial'
                ELSE 'added'
            END AS status
        FROM enriched_windowed w
        WHERE w.prev_yq IS NULL                    -- no prior row at all
           OR w.prev_yq != w.yq - w.prev_delta     -- prior row isn't the immediately preceding quarter
    ),
    removed AS (
        SELECT
            domain_id,
            store_app,
            tag_source,
            CASE WHEN quarter = 4 THEN year + 1 ELSE CAST(year AS INTEGER) END AS year,
            CASE WHEN quarter = 4 THEN 1          ELSE quarter + 1           END AS quarter,
            'removed' AS status
        FROM enriched_windowed
        WHERE (next_yq IS NULL OR next_yq != yq + next_delta)  -- next quarter is missing
          AND (yq + next_delta) <= max_yq                       -- don't emit "removed" past the last observed period
    )
    SELECT domain_id, store_app, tag_source, year, quarter, status FROM added
    UNION ALL
    SELECT domain_id, store_app, tag_source, year, quarter, status FROM removed
) TO '/tmp/domain_app_changes_quarterly.parquet'
(
    FORMAT PARQUET,
    ROW_GROUP_SIZE 100000,
    COMPRESSION 'zstd',
    OVERWRITE_OR_IGNORE true
);
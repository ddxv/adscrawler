WITH latest_descriptions AS (
    SELECT DISTINCT ON
    (sad.store_app)
        sad.id AS description_id,
        sad.store_app,
        sad.description_short,
        sad.description,
        sad.updated_at AS description_last_updated
    FROM
        store_apps_descriptions AS sad
    WHERE
        sad.language_id = 1
    ORDER BY
        sad.store_app ASC,
        sad.updated_at DESC
),
latest_extractions AS (
    SELECT DISTINCT ON
    (ak.store_app)
        ak.store_app,
        ak.extracted_at AS last_extracted_at
    FROM
        app_keywords_extracted AS ak
    ORDER BY
        ak.store_app ASC,
        ak.extracted_at DESC
),
base AS (
    SELECT
        ld.store_app,
        ld.description_id,
        le.last_extracted_at,
        ld.description_short,
        ld.description
    FROM latest_descriptions AS ld
    LEFT JOIN
        latest_extractions AS le
        ON
            ld.store_app = le.store_app
    WHERE le.last_extracted_at IS NULL OR (
        ld.description_last_updated > le.last_extracted_at
        AND le.last_extracted_at <= NOW() - INTERVAL '7 days'
    )
)
SELECT
    b.store_app,
    b.description_id,
    b.last_extracted_at,
    b.description_short,
    b.description
FROM
    base AS b
INNER JOIN app_global_metrics_latest AS agml ON b.store_app = agml.store_app
ORDER BY
    (CASE WHEN b.last_extracted_at IS NULL THEN 1 ELSE 0 END) DESC, -- always crawl new ones first
    (
        GREATEST(
            COALESCE(agml.installs, 0),
            COALESCE(agml.rating_count::BIGINT, 0)
        )
        * (10 * COALESCE(EXTRACT(DAY FROM (NOW() - b.last_extracted_at)), 1))
    ) DESC
LIMIT :mylimit;

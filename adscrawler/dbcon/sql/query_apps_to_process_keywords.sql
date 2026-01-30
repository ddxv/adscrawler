WITH latest_descriptions AS (
    SELECT DISTINCT ON (sad.store_app)
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
    (adke.description_id)
        adke.description_id,
        adke.store_app,
        adke.extracted_at AS app_keywords_extracted_at
    FROM
        logging.app_description_keywords_extracted AS adke
    ORDER BY
        adke.description_id ASC,
        adke.extracted_at DESC
),
base AS (
    SELECT
        ld.store_app,
        ld.description_id,
        le.app_keywords_extracted_at,
        ld.description_short,
        ld.description
    FROM latest_descriptions AS ld
    LEFT JOIN
        latest_extractions AS le
        ON
            ld.description_id = le.description_id
    WHERE
        le.app_keywords_extracted_at IS NULL
        OR ld.description_last_updated > le.app_keywords_extracted_at
        OR le.app_keywords_extracted_at <= NOW() - INTERVAL '31 days'
)
SELECT
    b.store_app,
    b.description_id,
    b.app_keywords_extracted_at,
    b.description_short,
    b.description
FROM
    base AS b
INNER JOIN app_global_metrics_latest AS agml ON b.store_app = agml.store_app
ORDER BY
    (CASE WHEN b.app_keywords_extracted_at IS NULL THEN 1 ELSE 0 END) DESC, -- always crawl new ones first
    (
        GREATEST(
            COALESCE(agml.installs, 0),
            COALESCE(agml.rating_count::BIGINT, 0)
        )
        * (
            10
            * COALESCE(
                EXTRACT(DAY FROM (NOW() - b.app_keywords_extracted_at)), 1
            )
        )
    ) DESC
LIMIT :mylimit;

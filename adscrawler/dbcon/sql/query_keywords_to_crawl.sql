WITH rank_crawled_keywords AS (
    SELECT DISTINCT akr.keyword_id
    FROM
        frontend.app_keyword_ranks_daily AS akr
    WHERE
        akr.crawled_date > CURRENT_DATE - INTERVAL '7 days'
),
log_crawled_keywords AS (
    SELECT DISTINCT keyword
    FROM
        logging.keywords_crawled_at
    WHERE
        crawled_at > CURRENT_DATE - INTERVAL '7 days'
),
scheduled_keywords AS (
    SELECT
        keyword_id,
        keyword_text,
        app_count,
        total_apps
    FROM
        frontend.keyword_scores
    WHERE
        keyword_id NOT IN (
            SELECT rck.keyword_id
            FROM
                rank_crawled_keywords AS rck
        )
        OR keyword_id NOT IN (
            SELECT lck.keyword
            FROM
                log_crawled_keywords AS lck
        )
    ORDER BY
        competitiveness_score
        DESC
),
distinct_sq AS (
    SELECT DISTINCT search_term
    FROM
        agadmin.search_queries
)
SELECT
    k.id AS keyword_id,
    k.keyword_text,
    'user' AS priority,
    0 AS app_count,
    0 AS total_apps
FROM
    distinct_sq AS sq
LEFT JOIN keywords AS k
    ON
        sq.search_term = k.keyword_text
WHERE
    k.id NOT IN (
        SELECT lck.keyword
        FROM
            log_crawled_keywords AS lck
    )
UNION ALL
SELECT
    keyword_id,
    keyword_text,
    'scheduled' AS priority,
    app_count,
    total_apps
FROM
    scheduled_keywords
LIMIT :mylimit;

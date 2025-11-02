WITH rank_crawled_keywords AS (
    SELECT DISTINCT akr.keyword
    FROM
        app_keyword_rankings AS akr
    WHERE
        akr.crawled_date > CURRENT_DATE - INTERVAL '7 days'
),
log_crawled_keywords AS (
    SELECT DISTINCT keyword
    FROM
        logging.keywords_crawled_at
    WHERE
        crawled_at > CURRENT_DATE - INTERVAL '7 days'
)
SELECT
    store,
    keyword_id,
    keyword_text,
    app_count,
    total_apps,
    competitiveness_score
FROM
    frontend.keyword_scores
WHERE
    keyword_id NOT IN (
        SELECT rck.keyword
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
LIMIT :mylimit;


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
        crawled_at > CURRENT_DATE - INTERVAL '3 days'
),
scheduled_keywords AS (
    SELECT
        ks.keyword_id,
        ks.keyword_text,
        sum(ks.app_count) AS app_count
    FROM
        frontend.keyword_scores AS ks
    WHERE
        ks.keyword_id IN (SELECT kb.keyword_id FROM keywords_base AS kb)
        AND
        (
            ks.keyword_id NOT IN (
                SELECT rck.keyword_id
                FROM
                    rank_crawled_keywords AS rck
            )
            AND ks.keyword_id NOT IN (
                SELECT lck.keyword
                FROM
                    log_crawled_keywords AS lck
            )
        )
    GROUP BY ks.keyword_id, ks.keyword_text
    ORDER BY
        app_count
        DESC
),
distinct_sq AS (
    SELECT DISTINCT search_term
    FROM
        agadmin.search_queries
),
allresults AS (SELECT
    k.id AS keyword_id,
    k.keyword_text,
    'user' AS priority,
    0 AS app_count
FROM
    distinct_sq AS sq
LEFT JOIN keywords AS k
    ON
        sq.search_term = k.keyword_text
WHERE
    k.id IS NOT NULL
        AND
        (
            k.id NOT IN (
                SELECT rck.keyword_id
                FROM
                    rank_crawled_keywords AS rck
            )
         AND k.id NOT IN (
                SELECT lck.keyword
                FROM
                    log_crawled_keywords AS lck
            )
        )
UNION ALL
SELECT
    sk.keyword_id,
    sk.keyword_text,
    'scheduled' AS priority,
    sk.app_count
FROM
    scheduled_keywords AS sk
)
SELECT
    keyword_id,
    keyword_text,
    app_count,
    count(*) OVER () AS total_queue_depth
FROM
    allresults
LIMIT :mylimit;
WITH mydvs AS (
    SELECT
        d.id,
        d.developer_id,
        dc.apps_crawled_at,
        SUM(sa.installs) AS total_installs
    FROM
        developers AS d
    LEFT JOIN logging.developers_crawled_at AS dc
        ON d.id = dc.developer
    LEFT JOIN frontend.store_apps_overview AS sa
        ON d.developer_id = sa.developer_id
    WHERE
        d.store = :store
        AND (dc.apps_crawled_at <= :before_date OR dc.apps_crawled_at IS NULL)
        AND sa.crawl_result = 1
    GROUP BY
        d.id, dc.apps_crawled_at
    ORDER BY
        dc.apps_crawled_at::date NULLS FIRST,
        sa.installs DESC NULLS LAST
)
SELECT
    id,
    developer_id,
    total_installs,
    apps_crawled_at
FROM mydvs
LIMIT :mylimit;

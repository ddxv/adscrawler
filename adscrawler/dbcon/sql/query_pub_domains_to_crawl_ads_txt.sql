WITH myq AS (
    SELECT
        pd.id,
        pd.domain_name AS url,
        bool_or(coalesce(sa.ad_supported, FALSE)) AS ad_supported,
        max(pdcr.crawled_at) AS crawled_at
    FROM
        app_urls_map AS aum
    LEFT JOIN domains AS pd
        ON
            aum.pub_domain = pd.id
    LEFT JOIN adstxt_crawl_results AS pdcr
        ON
            (
                pd.id = pdcr.domain_id
            )
    LEFT JOIN store_apps AS sa
        ON
            aum.store_app = sa.id
    WHERE
        sa.crawl_result = 1
        AND
        (
            (
                -- crawl ad_supported more often
                sa.ad_supported
                AND (
                    pdcr.crawled_at <= cast(:short_update_ts AS timestamp)
                    OR pdcr.crawled_at IS NULL
                )
            )
        -- crawl all apps domains occassionally
        -- OR (
        -- 	pdcr.crawled_at <= CAST(:max_recrawl_ts AS timestamp)
        -- 		OR pdcr.crawled_at IS NULL
        -- )
        )
    GROUP BY
        pd.id,
        pd.domain_name
)
SELECT
    id,
    url,
    ad_supported,
    crawled_at
FROM
    myq
ORDER BY
    ad_supported DESC,
    crawled_at ASC NULLS FIRST
LIMIT :mylimit;

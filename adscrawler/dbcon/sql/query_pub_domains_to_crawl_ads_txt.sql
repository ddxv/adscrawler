WITH myq AS (
	SELECT
		pd.id,
		pd.domain_name AS url,
		bool_or(COALESCE(sa.ad_supported, FALSE)) AS ad_supported,
		max(pdcr.crawled_at) AS crawled_at
	FROM
		app_urls_map aum
	LEFT JOIN domains pd ON
		pd.id = aum.pub_domain
	LEFT JOIN adstxt_crawl_results pdcr ON
		(
			pd.id = pdcr.domain_id
		)
	LEFT JOIN store_apps sa ON
		sa.id = aum.store_app
	WHERE
		sa.crawl_result = 1
		AND
    (
			(
				-- crawl ad_supported more often
				sa.ad_supported
					AND (
						pdcr.crawled_at <= CAST(:short_update_ts AS timestamp)
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
			id, url, ad_supported, crawled_at
FROM
			myq
ORDER BY
			ad_supported DESC,
			crawled_at ASC NULLS FIRST
LIMIT :mylimit;

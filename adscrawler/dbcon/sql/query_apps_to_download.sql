WITH latest_version_codes AS (
    SELECT DISTINCT ON
    (store_app)
        id,
        store_app,
        version_code,
        updated_at,
        crawl_result
    FROM
        version_codes
    ORDER BY
        store_app ASC,
        updated_at DESC,
        string_to_array(version_code, '.')::bigint [] DESC
),

latest_success_version_codes AS (
    SELECT DISTINCT ON
    (store_app)
        id,
        store_app,
        version_code,
        updated_at,
        crawl_result
    FROM
        version_codes
    WHERE
        crawl_result = 1
    ORDER BY
        store_app ASC,
        updated_at DESC,
        string_to_array(version_code, '.')::bigint [] DESC
),

failing_downloads AS (
    SELECT
        store_app,
        count(*) AS attempt_count
    FROM
        logging.store_app_downloads
    WHERE
        crawl_result != 1
        AND updated_at >= current_date - interval '30 days'
    GROUP BY
        store_app
),

scheduled_apps_crawl AS (
    SELECT
        dc.store_app,
        dc.store_id,
        dc.name,
        dc.installs,
        dc.rating_count,
        vc.crawl_result AS last_crawl_result,
        vc.updated_at AS last_download_attempt,
        lsvc.updated_at AS last_downloaded_at,
        coalesce(fd.attempt_count, 0) AS attempt_count
    FROM
        store_apps_in_latest_rankings AS dc
    LEFT JOIN latest_version_codes AS vc
        ON
            dc.store_app = vc.store_app
    LEFT JOIN latest_success_version_codes AS lsvc
        ON
            dc.store_app = lsvc.store_app
    LEFT JOIN failing_downloads AS fd
        ON
            vc.store_app = fd.store_app
    WHERE
        dc.store = :store
        AND
        (
            vc.updated_at IS NULL
            OR
            (
                (
                    vc.crawl_result = 1
                    AND (
                        vc.updated_at < current_date - interval '180 days'
                        OR vc.updated_at < '2025-05-01'
                    )
                )
                OR
                (
                    lsvc.updated_at IS NULL
                    AND vc.crawl_result IN (
                        2, 3, 4
                    )
                    AND vc.updated_at < current_date - interval '30 days'
                )
            )
        )
),

user_requested_apps_crawl AS (
    SELECT DISTINCT ON
    (sa.id)
        sa.id AS store_app,
        sa.store_id,
        sa.name,
        sa.installs,
        sa.rating_count,
        urs.created_at AS user_last_requested,
        lvc.crawl_result AS last_crawl_result,
        lvc.updated_at AS last_download_attempt,
        lsvc.updated_at AS last_downloaded_at,
        coalesce(fd.attempt_count, 0) AS attempt_count
    FROM
        user_requested_scan AS urs
    LEFT JOIN store_apps AS sa
        ON
            urs.store_id = sa.store_id
    LEFT JOIN latest_success_version_codes AS lsvc
        ON
            sa.id = lsvc.store_app
    LEFT JOIN latest_version_codes AS lvc
        ON
            sa.id = lvc.store_app
    LEFT JOIN failing_downloads AS fd
        ON
            sa.id = fd.store_app
    WHERE
        (
            (
                lsvc.updated_at < urs.created_at
                AND (
                    (
                        sa.store_last_updated > lsvc.updated_at
                        AND lsvc.updated_at > '2025-05-01'
                    )
                    OR lsvc.updated_at < '2025-05-01'
                )
            )
            OR lsvc.updated_at IS NULL
        )
        AND (
            lvc.updated_at < current_date - interval '1 days'
            OR lvc.updated_at IS NULL
        )
        AND sa.store = :store
        AND sa.free
        -- random apps requested directly BY users NOT FOUND ON store
        AND sa.name IS NOT NULL
    ORDER BY
        sa.id ASC,
        urs.created_at DESC
),

combined AS (
    SELECT
        store_app,
        store_id,
        name,
        installs,
        rating_count,
        attempt_count,
        last_crawl_result,
        'user' AS mysource,
        last_download_attempt,
        last_downloaded_at
    FROM
        user_requested_apps_crawl
    WHERE
        attempt_count < 3
    UNION ALL
    SELECT
        store_app,
        store_id,
        name,
        installs,
        rating_count,
        attempt_count,
        last_crawl_result,
        'scheduled' AS mysource,
        last_download_attempt,
        last_downloaded_at
    FROM
        scheduled_apps_crawl
    WHERE
        attempt_count < 2
        AND (
            last_downloaded_at IS NULL
            OR last_downloaded_at < current_date - interval '180 days'
        )
),

final_selection AS (
    SELECT
        *,
        coalesce(date_part('day', current_date - last_downloaded_at), 1000)
        * greatest(
            coalesce(installs, 0),
            coalesce(rating_count::bigint, 0) * 50
        ) AS mynum,
        row_number() OVER (
            ORDER BY
                mysource DESC,
                coalesce(
                    date_part('day', current_date - last_downloaded_at), 1000
                )
                * greatest(
                    coalesce(installs, 0),
                    coalesce(rating_count::bigint, 0) * 50
                ) DESC NULLS LAST
        ) AS app_rank
    FROM
        combined
)

SELECT *
FROM
    final_selection;

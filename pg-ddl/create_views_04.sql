CREATE MATERIALIZED VIEW store_apps_networks AS
WITH latest_version_codes AS (
    SELECT
        id,
        store_app,
        MAX(version_code) AS version_code
    FROM
        version_codes
    WHERE
        crawl_result = 1
    GROUP BY
        id,
        store_app
),

apps_with_networks AS (
    SELECT DISTINCT
        vc.store_app,
        tm.network
    FROM
        latest_version_codes AS vc
    LEFT JOIN version_details AS vd
        ON
            vc.id = vd.version_code
    INNER JOIN network_package_map AS tm ON
        vd.android_name ~* tm.package_pattern
)

SELECT
    awt.store_app,
    awt.network
FROM
    apps_with_networks AS awt
UNION ALL
SELECT
    vc.store_app,
    -- Note: 17 is tracker db id for no network found
    17 AS network
FROM
    latest_version_codes AS vc
WHERE
    vc.store_app NOT IN (
        SELECT store_app
        FROM
            apps_with_networks
    )
WITH DATA;

DROP INDEX IF EXISTS idx_store_apps_networks;
CREATE UNIQUE INDEX idx_store_apps_networks
ON store_apps_networks (store_app, network);

CREATE MATERIALIZED VIEW store_apps_trackers AS
WITH latest_version_codes AS (
    SELECT
        id,
        store_app,
        MAX(version_code) AS version_code
    FROM
        version_codes
    WHERE
        crawl_result = 1
    GROUP BY
        id,
        store_app
),

apps_with_trackers AS (
    SELECT DISTINCT
        vc.store_app,
        tm.tracker
    FROM
        latest_version_codes AS vc
    LEFT JOIN version_details AS vd
        ON
            vc.id = vd.version_code
    INNER JOIN tracker_package_map AS tm ON
        vd.android_name ~* tm.package_pattern
)

SELECT
    awt.store_app,
    awt.tracker
FROM
    apps_with_trackers AS awt
UNION ALL
SELECT
    vc.store_app,
    -- Note: 10 is tracker db id for no tracker found
    10 AS tracker
FROM
    latest_version_codes AS vc
WHERE
    vc.store_app NOT IN (
        SELECT store_app
        FROM
            apps_with_trackers
    )
WITH DATA;

DROP INDEX IF EXISTS idx_store_apps_trackers;
CREATE UNIQUE INDEX idx_store_apps_trackers
ON store_apps_trackers (store_app, tracker);

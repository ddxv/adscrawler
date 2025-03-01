-- public.audit_dates source

CREATE MATERIALIZED VIEW public.audit_dates
TABLESPACE pg_default
AS WITH sa AS (
    SELECT
        store_apps_audit.stamp::date AS updated_date,
        'store_apps'::text AS table_name,
        count(*) AS updated_count
    FROM logging.store_apps_audit
    GROUP BY (store_apps_audit.stamp::date)
)

SELECT
    sa.updated_date,
    sa.table_name,
    sa.updated_count
FROM sa
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX audit_dates_updated_date_idx
ON public.audit_dates USING btree (
    updated_date, table_name
);

--for internal dashboard checking last_updated_at and created_ats
--DROP MATERIALIZED VIEW store_apps_updated_at;
CREATE MATERIALIZED VIEW store_apps_updated_at AS
WITH my_dates AS (
    SELECT
        store,
        generate_series(
            current_date - interval '365 days',
            current_date,
            '1 day'::interval
        )::date AS date
    FROM
        generate_series(
            1,
            2,
            1
        ) AS num_series (store)
),

updated_dates AS (
    SELECT
        store,
        updated_at::date AS last_updated_date,
        count(*) AS last_updated_count
    FROM
        store_apps
    WHERE
        updated_at >= current_date - interval '365 days'
    GROUP BY
        store,
        updated_at::date
)

SELECT
    my_dates.store,
    my_dates.date,
    updated_dates.last_updated_count,
    audit_dates.updated_count
FROM
    my_dates
LEFT JOIN updated_dates
    ON
        my_dates.date = updated_dates.last_updated_date
        AND my_dates.store = updated_dates.store
LEFT JOIN audit_dates
    ON
        my_dates.date = audit_dates.updated_date
ORDER BY
    my_dates.date DESC;


CREATE UNIQUE INDEX idx_store_apps_updated_at
ON store_apps_updated_at (store, date);


CREATE MATERIALIZED VIEW store_apps_created_at AS
WITH my_dates AS (
    SELECT
        store,
        generate_series(
            current_date - interval '365 days',
            current_date,
            '1 day'::interval
        )::date AS date
    FROM
        generate_series(
            1,
            2,
            1
        ) AS num_series (store)
),

created_dates AS (
    SELECT
        sa.store,
        sa.created_at::date AS created_date,
        sas.crawl_source,
        count(*) AS created_count
    FROM
        store_apps AS sa
    LEFT JOIN logging.store_app_sources AS sas
        ON
            sa.id = sas.store_app
            AND sa.store = sas.store
    WHERE
        sa.created_at >= current_date - interval '365 days'
    GROUP BY
        sa.store,
        sa.created_at::date,
        sas.crawl_source
)

SELECT
    my_dates.store,
    my_dates.date,
    created_dates.crawl_source,
    created_dates.created_count
FROM
    my_dates
LEFT JOIN created_dates ON
    my_dates.date = created_dates.created_date
    AND my_dates.store = created_dates.store;

CREATE UNIQUE INDEX idx_store_apps_created_at
ON store_apps_created_at (store, date, crawl_source);

--
-- PostgreSQL database dump
--

\restrict wh5b4pr7bnTgG6cOclwaVCAhhDnZ6YxUQCB6p3Le9WIwGLYHJ9uKauZYx0r0tgC

-- Dumped from database version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)
-- Dumped by pg_dump version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: store_apps_history_weekly; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_history_weekly AS
WITH date_diffs AS (
    SELECT
        sach.store_app,
        sach.country_id,
        sach.crawled_date,
        sach.installs,
        sach.rating_count,
        max(sach.crawled_date)
            OVER (PARTITION BY sach.store_app, sach.country_id)
            AS last_date,
        (
            sach.installs
            - lead(sach.installs)
                OVER (
                    PARTITION BY sach.store_app, sach.country_id
                    ORDER BY sach.crawled_date DESC
                )
        ) AS installs_diff,
        (
            sach.rating_count
            - lead(sach.rating_count)
                OVER (
                    PARTITION BY sach.store_app, sach.country_id
                    ORDER BY sach.crawled_date DESC
                )
        ) AS rating_count_diff,
        (
            sach.crawled_date
            - lead(sach.crawled_date)
                OVER (
                    PARTITION BY sach.store_app, sach.country_id
                    ORDER BY sach.crawled_date DESC
                )
        ) AS days_diff
    FROM public.store_apps_country_history AS sach
    WHERE ((sach.store_app IN (
        SELECT sa.id
        FROM public.store_apps AS sa
        WHERE (sa.crawl_result = 1)
    )) AND (sach.crawled_date > (current_date - '375 days'::interval
    )))
), weekly_totals AS (
    SELECT
        (
            date_trunc(
                'week'::text,
                (date_diffs.crawled_date)::timestamp with time zone
            )
        )::date AS week_start,
        date_diffs.store_app,
        date_diffs.country_id,
        sum(date_diffs.installs_diff) AS installs_diff,
        sum(date_diffs.rating_count_diff) AS rating_count_diff,
        sum(date_diffs.days_diff) AS days_diff
    FROM date_diffs
    GROUP BY
        (
            (
                date_trunc(
                    'week'::text,
                    (date_diffs.crawled_date)::timestamp with time zone
                )
            )::date
        ),
        date_diffs.store_app,
        date_diffs.country_id
)
SELECT
    week_start,
    store_app,
    country_id,
    installs_diff,
    rating_count_diff
FROM weekly_totals
ORDER BY week_start DESC, store_app ASC, country_id ASC
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_history_weekly OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict wh5b4pr7bnTgG6cOclwaVCAhhDnZ6YxUQCB6p3Le9WIwGLYHJ9uKauZYx0r0tgC

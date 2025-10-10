--
-- PostgreSQL database dump
--

\restrict VhymecTz1pqZ6HUIeMFMcgaiJVIVrwSAdPjydV8tvK5ACimczgU2goOOmcKkbV6

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
-- Name: store_apps_updated_at; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_updated_at AS
WITH my_dates AS (
    SELECT
        num_series.store,
        (
            generate_series(
                (current_date - '365 days'::interval),
                (current_date)::timestamp without time zone,
                '1 day'::interval
            )
        )::date AS date
    FROM generate_series(1, 2, 1) AS num_series (store)
), updated_dates AS (
    SELECT
        store_apps.store,
        (store_apps.updated_at)::date AS last_updated_date,
        count(*) AS last_updated_count
    FROM public.store_apps
    WHERE (store_apps.updated_at >= (current_date - '365 days'::interval))
    GROUP BY store_apps.store, ((store_apps.updated_at)::date)
)
SELECT
    my_dates.store,
    my_dates.date,
    updated_dates.last_updated_count,
    audit_dates.updated_count
FROM ((
    my_dates
    LEFT JOIN
        updated_dates
        ON
            (
                (
                    (my_dates.date = updated_dates.last_updated_date)
                    AND (my_dates.store = updated_dates.store)
                )
            )
)
LEFT JOIN public.audit_dates ON ((my_dates.date = audit_dates.updated_date)))
ORDER BY my_dates.date DESC
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_updated_at OWNER TO postgres;

--
-- Name: idx_my_materialized_view_store_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_my_materialized_view_store_date ON public.store_apps_updated_at USING btree (
    store, date
);


--
-- Name: idx_store_apps_updated_at; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_apps_updated_at ON public.store_apps_updated_at USING btree (
    store, date
);


--
-- PostgreSQL database dump complete
--

\unrestrict VhymecTz1pqZ6HUIeMFMcgaiJVIVrwSAdPjydV8tvK5ACimczgU2goOOmcKkbV6

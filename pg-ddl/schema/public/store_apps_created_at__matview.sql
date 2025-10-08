--
-- PostgreSQL database dump
--

\restrict 9sCXnCJY0jnhahhmJL8jfBcbj3AXWNBJ8XKQbuAttU29JgyNXjYSKZ23PGqfZ3b

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
-- Name: store_apps_created_at; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_created_at AS
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
), created_dates AS (
    SELECT
        sa.store,
        (sa.created_at)::date AS created_date,
        sas.crawl_source,
        count(*) AS created_count
    FROM (
        public.store_apps AS sa
        LEFT JOIN
            logging.store_app_sources AS sas
            ON (((sa.id = sas.store_app) AND (sa.store = sas.store)))
    )
    WHERE (sa.created_at >= (current_date - '365 days'::interval))
    GROUP BY sa.store, ((sa.created_at)::date), sas.crawl_source
)
SELECT
    my_dates.store,
    my_dates.date,
    created_dates.crawl_source,
    created_dates.created_count
FROM (
    my_dates
    LEFT JOIN
        created_dates
        ON
            (
                (
                    (my_dates.date = created_dates.created_date)
                    AND (my_dates.store = created_dates.store)
                )
            )
)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_created_at OWNER TO postgres;

--
-- Name: idx_store_apps_created_at; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_store_apps_created_at ON public.store_apps_created_at USING btree (
    store, date, crawl_source
);


--
-- Name: idx_store_apps_created_atx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_apps_created_atx ON public.store_apps_created_at USING btree (
    store, date, crawl_source
);


--
-- PostgreSQL database dump complete
--

\unrestrict 9sCXnCJY0jnhahhmJL8jfBcbj3AXWNBJ8XKQbuAttU29JgyNXjYSKZ23PGqfZ3b

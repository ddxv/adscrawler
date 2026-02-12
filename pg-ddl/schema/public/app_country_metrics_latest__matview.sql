--
-- PostgreSQL database dump
--

\restrict AO390YDhbrRVDRwmAmeuHYQEBSYE3MLww1nEb1Ue2gM87VtYV8bcbBIDuK429Zq

-- Dumped from database version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)
-- Dumped by pg_dump version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)

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
-- Name: app_country_metrics_latest; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.app_country_metrics_latest AS
 SELECT DISTINCT ON (store_app, country_id) snapshot_date,
    store_app,
    country_id,
    review_count,
    rating,
    rating_count,
    one_star,
    two_star,
    three_star,
    four_star,
    five_star
   FROM public.app_country_metrics_history sacs
  ORDER BY store_app, country_id, snapshot_date DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.app_country_metrics_latest OWNER TO postgres;

--
-- Name: app_country_metrics_latest_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_country_metrics_latest_idx ON public.app_country_metrics_latest USING btree (store_app, country_id);


--
-- PostgreSQL database dump complete
--

\unrestrict AO390YDhbrRVDRwmAmeuHYQEBSYE3MLww1nEb1Ue2gM87VtYV8bcbBIDuK429Zq


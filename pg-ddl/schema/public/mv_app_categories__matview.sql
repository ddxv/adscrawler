--
-- PostgreSQL database dump
--

\restrict onThzYoBQWebDd7asVBiVCE0fmce8bfHum7pnVcIWkxin4tUg4u1iHccPgdFRaC

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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
-- Name: mv_app_categories; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.mv_app_categories AS
 SELECT store,
    category,
    count(*) AS app_count
   FROM frontend.store_apps_overview sa
  WHERE ((category IS NOT NULL) AND (NOT is_removed))
  GROUP BY store, category
  ORDER BY store, category
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_app_categories OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict onThzYoBQWebDd7asVBiVCE0fmce8bfHum7pnVcIWkxin4tUg4u1iHccPgdFRaC


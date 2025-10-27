--
-- PostgreSQL database dump
--

\restrict 7KdkbsDnoy2oBA3PR5LmlFdmh0jIwvBcQgaCzdcpA5ImLtXOAwK7pLcnKEUxEgt

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
SELECT
    sa.store,
    cm.mapped_category AS category,
    count(*) AS app_count
FROM (
    public.store_apps AS sa
    INNER JOIN
        public.category_mapping AS cm
        ON (((sa.category)::text = (cm.original_category)::text))
)
WHERE ((sa.crawl_result = 1) AND (sa.category IS NOT null))
GROUP BY sa.store, cm.mapped_category
ORDER BY sa.store, cm.mapped_category
WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_app_categories OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict 7KdkbsDnoy2oBA3PR5LmlFdmh0jIwvBcQgaCzdcpA5ImLtXOAwK7pLcnKEUxEgt

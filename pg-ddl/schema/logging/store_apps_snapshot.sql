--
-- PostgreSQL database dump
--

\restrict pDkqdbRuabBlZaHzcT2HZxhENnZJX1c38vxXJAKnhNT9g1qf1sU55e5PM9JjVzr

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
-- Name: store_apps_snapshot; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_snapshot (
    updated_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL,
    store integer,
    crawl_result integer,
    total_rows integer,
    avg_days numeric,
    max_days bigint,
    rows_older_than15 integer
);


ALTER TABLE logging.store_apps_snapshot OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict pDkqdbRuabBlZaHzcT2HZxhENnZJX1c38vxXJAKnhNT9g1qf1sU55e5PM9JjVzr

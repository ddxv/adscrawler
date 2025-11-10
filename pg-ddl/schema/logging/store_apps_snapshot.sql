--
-- PostgreSQL database dump
--

\restrict wmtUV9yCW1Kszn6txwI9hBlzQGRTbB7kCDlbxaPJNr0qzHcIJVnXbJwBmvY8neM

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
-- Name: store_apps_snapshot; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_snapshot (
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
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

\unrestrict wmtUV9yCW1Kszn6txwI9hBlzQGRTbB7kCDlbxaPJNr0qzHcIJVnXbJwBmvY8neM


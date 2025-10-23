--
-- PostgreSQL database dump
--

\restrict Zs2tXgYEbhNhXa9fWOHH8I4M364z6VOYuqd5C6I9xK5CqyUVLxiuJa6y3ukLA8R

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
-- Name: creative_scan_results; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.creative_scan_results (
    url text,
    tld_url text,
    path text,
    content_type text,
    run_id text,
    pub_store_id text,
    file_extension text,
    creative_size bigint,
    error_msg text,
    inserted_at character varying DEFAULT now() NOT NULL
);


ALTER TABLE logging.creative_scan_results OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict Zs2tXgYEbhNhXa9fWOHH8I4M364z6VOYuqd5C6I9xK5CqyUVLxiuJa6y3ukLA8R

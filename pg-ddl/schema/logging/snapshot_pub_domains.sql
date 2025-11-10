--
-- PostgreSQL database dump
--

\restrict xFlp2n9Y4auNRUi3lkhUdmq6DcWMiEE6y1Z9Iak8cfZXNmfl767TzgwZQEEi4U0

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
-- Name: snapshot_pub_domains; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.snapshot_pub_domains (
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    crawl_result integer,
    total_rows integer,
    avg_days numeric,
    max_days bigint,
    rows_older_than15 integer
);


ALTER TABLE logging.snapshot_pub_domains OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict xFlp2n9Y4auNRUi3lkhUdmq6DcWMiEE6y1Z9Iak8cfZXNmfl767TzgwZQEEi4U0


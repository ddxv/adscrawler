--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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

--
-- Name: logging; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA logging;


ALTER SCHEMA logging OWNER TO postgres;

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
-- Name: developers_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.developers_crawled_at (
    developer integer NOT NULL,
    apps_crawled_at timestamp without time zone
);


ALTER TABLE logging.developers_crawled_at OWNER TO postgres;

--
-- Name: keywords_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.keywords_crawled_at (
    keyword integer NOT NULL,
    crawled_at timestamp without time zone NOT NULL
);


ALTER TABLE logging.keywords_crawled_at OWNER TO postgres;

--
-- Name: snapshot_pub_domains; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.snapshot_pub_domains (
    updated_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL,
    crawl_result integer,
    total_rows integer,
    avg_days numeric,
    max_days bigint,
    rows_older_than15 integer
);


ALTER TABLE logging.snapshot_pub_domains OWNER TO postgres;

--
-- Name: store_app_downloads; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_downloads (
    store_app integer NOT NULL,
    version_code text NOT NULL,
    crawl_result smallint NOT NULL,
    updated_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL
);


ALTER TABLE logging.store_app_downloads OWNER TO postgres;

--
-- Name: store_app_no_creatives; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_no_creatives (
    store_app_id bigint,
    pub_store_id text,
    run_id text
);


ALTER TABLE logging.store_app_no_creatives OWNER TO postgres;

--
-- Name: store_app_sources; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_sources (
    store smallint NOT NULL,
    store_app integer NOT NULL,
    crawl_source text
);


ALTER TABLE logging.store_app_sources OWNER TO postgres;

--
-- Name: store_app_waydroid_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_waydroid_crawled_at (
    store_app integer NOT NULL,
    crawl_result smallint NOT NULL,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.store_app_waydroid_crawled_at OWNER TO postgres;

--
-- Name: store_apps_audit; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_audit (
    operation character(1) NOT NULL,
    stamp timestamp without time zone NOT NULL,
    userid text NOT NULL,
    row_id bigint NOT NULL,
    store smallint NOT NULL,
    store_id text NOT NULL,
    crawl_result integer
);


ALTER TABLE logging.store_apps_audit OWNER TO postgres;

--
-- Name: store_apps_crawl; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_crawl (
    index bigint,
    crawl_result bigint,
    store bigint,
    store_id text,
    store_app bigint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.store_apps_crawl OWNER TO postgres;

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
-- Name: version_code_api_scan_results; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.version_code_api_scan_results (
    store_app bigint NOT NULL,
    version_code text,
    apk_hash text,
    crawl_result smallint,
    updated_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL
);


ALTER TABLE logging.version_code_api_scan_results OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

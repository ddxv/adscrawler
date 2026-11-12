--
-- PostgreSQL database dump
--

\restrict dXyEs72RKWh46c6XcfSzNFwf5x6nSOUljy0P3mAeiighqC0knQ5HGsK2dZpQIt1

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

--
-- Name: app_keyword_ranks_daily; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.app_keyword_ranks_daily (
    crawled_date date CONSTRAINT app_keyword_ranks_daily_v2_crawled_date_not_null NOT NULL,
    store smallint CONSTRAINT app_keyword_ranks_daily_v2_store_not_null NOT NULL,
    country smallint CONSTRAINT app_keyword_ranks_daily_v2_country_not_null NOT NULL,
    keyword_id integer CONSTRAINT app_keyword_ranks_daily_v2_keyword_id_not_null NOT NULL,
    store_app integer CONSTRAINT app_keyword_ranks_daily_v2_store_app_not_null NOT NULL,
    app_rank smallint CONSTRAINT app_keyword_ranks_daily_v2_app_rank_not_null NOT NULL
)
PARTITION BY RANGE (crawled_date);


ALTER TABLE frontend.app_keyword_ranks_daily OWNER TO postgres;

--
-- Name: app_keyword_ranks_daily_lookup; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_lookup ON ONLY frontend.app_keyword_ranks_daily USING btree (store_app, crawled_date);


--
-- PostgreSQL database dump complete
--

\unrestrict dXyEs72RKWh46c6XcfSzNFwf5x6nSOUljy0P3mAeiighqC0knQ5HGsK2dZpQIt1


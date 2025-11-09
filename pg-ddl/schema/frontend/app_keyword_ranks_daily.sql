--
-- PostgreSQL database dump
--

\restrict ZOGd1Icm0aDKNJxSwQxA7vAwjYaHMlrg1ye5PXWD4knlQf42hLOZyQgKNDEFTdy

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
-- Name: app_keyword_ranks_daily; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.app_keyword_ranks_daily (
    crawled_date date NOT NULL,
    country smallint NOT NULL,
    app_rank smallint NOT NULL,
    keyword_id integer NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE frontend.app_keyword_ranks_daily OWNER TO postgres;

--
-- Name: app_keyword_ranks_daily app_keyword_rankings_unique_test; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT app_keyword_rankings_unique_test UNIQUE (crawled_date, country, keyword_id, app_rank);


--
-- Name: app_keyword_ranks_daily_app_lookup; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_app_lookup ON frontend.app_keyword_ranks_daily USING btree (crawled_date, store_app);


--
-- Name: app_keyword_ranks_daily_date; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_date ON frontend.app_keyword_ranks_daily USING btree (crawled_date);


--
-- PostgreSQL database dump complete
--

\unrestrict ZOGd1Icm0aDKNJxSwQxA7vAwjYaHMlrg1ye5PXWD4knlQf42hLOZyQgKNDEFTdy


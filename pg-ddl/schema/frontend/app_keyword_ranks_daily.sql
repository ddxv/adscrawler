--
-- PostgreSQL database dump
--

\restrict sRa1BqJR6CSXu1g3IlkCxFMkbzAxgJyPaMmPzpRI6InKr9OhlvFLJFrEDWQgnu3

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
    store smallint NOT NULL,
    country smallint NOT NULL,
    keyword_id integer NOT NULL,
    store_app integer NOT NULL,
    app_rank smallint NOT NULL
);


ALTER TABLE frontend.app_keyword_ranks_daily OWNER TO postgres;

--
-- Name: app_keyword_ranks_daily app_keyword_rankings_unique_test; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT app_keyword_rankings_unique_test UNIQUE (crawled_date, store, country, keyword_id, app_rank);


--
-- Name: app_keyword_ranks_daily_app_lookup; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_app_lookup ON frontend.app_keyword_ranks_daily USING btree (crawled_date, store_app);


--
-- Name: app_keyword_ranks_daily_date; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_date ON frontend.app_keyword_ranks_daily USING btree (crawled_date);


--
-- Name: app_keywords_delete_and_insert_on; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keywords_delete_and_insert_on ON frontend.app_keyword_ranks_daily USING btree (crawled_date, store);


--
-- Name: app_keyword_ranks_daily keyword_kr_fk; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT keyword_kr_fk FOREIGN KEY (keyword_id) REFERENCES public.keywords(id);


--
-- PostgreSQL database dump complete
--

\unrestrict sRa1BqJR6CSXu1g3IlkCxFMkbzAxgJyPaMmPzpRI6InKr9OhlvFLJFrEDWQgnu3


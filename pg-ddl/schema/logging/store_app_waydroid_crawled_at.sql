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

SET default_tablespace = '';

SET default_table_access_method = heap;

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
-- Name: logging_store_app_upsert_unique; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE UNIQUE INDEX logging_store_app_upsert_unique ON logging.store_app_waydroid_crawled_at USING btree (store_app, crawl_result, crawled_at);


--
-- Name: store_app_waydroid_crawled_at waydroid_crawl_result_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_waydroid_crawled_at
    ADD CONSTRAINT waydroid_crawl_result_fk FOREIGN KEY (crawl_result) REFERENCES public.crawl_results(id);


--
-- Name: store_app_waydroid_crawled_at waydroid_store_apps_crawl_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_waydroid_crawled_at
    ADD CONSTRAINT waydroid_store_apps_crawl_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- PostgreSQL database dump complete
--


--
-- PostgreSQL database dump
--

\restrict 3lXzJG5VFtYvogyb7qCpvuZ0jreDH5fPxgzbhvFNSY7hMvWnEau3WezaRyIAgYG

-- Dumped from database version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)
-- Dumped by pg_dump version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)

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
-- Name: store_app_ranks_weekly; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.store_app_ranks_weekly (
    rank smallint NOT NULL,
    best_rank smallint NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    crawled_date date NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE frontend.store_app_ranks_weekly OWNER TO postgres;

--
-- Name: store_app_ranks_weekly app_rankings_unique_test; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT app_rankings_unique_test UNIQUE (crawled_date, country, store_collection, store_category, rank);


--
-- Name: idx_ranks_filter; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_ranks_filter ON frontend.store_app_ranks_weekly USING btree (country, store_collection, store_category, crawled_date);


--
-- Name: sarw_crawled_store_collection_category_country_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX sarw_crawled_store_collection_category_country_idx ON frontend.store_app_ranks_weekly USING btree (crawled_date, store_app, store_collection, store_category, country);


--
-- Name: store_app_ranks_weekly fk_country; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: store_app_ranks_weekly fk_store_app; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: store_app_ranks_weekly fk_store_category; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_category FOREIGN KEY (store_category) REFERENCES public.store_categories(id);


--
-- Name: store_app_ranks_weekly fk_store_collection; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_collection FOREIGN KEY (store_collection) REFERENCES public.store_collections(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 3lXzJG5VFtYvogyb7qCpvuZ0jreDH5fPxgzbhvFNSY7hMvWnEau3WezaRyIAgYG


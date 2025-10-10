--
-- PostgreSQL database dump
--

\restrict o5D0NZ65v8XJjCygcWFaDGSw8DoeMbXYmXVOp9TPvf0DtSpQKWjrVC5sz5fLaZU

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
-- Name: store_app_ranks_daily; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.store_app_ranks_daily (
    rank smallint NOT NULL,
    best_rank smallint NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    crawled_date date NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE frontend.store_app_ranks_daily OWNER TO postgres;

--
-- Name: store_app_ranks_daily app_rankings_unique_daily; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
ADD CONSTRAINT app_rankings_unique_daily UNIQUE (
    crawled_date, country, store_collection, store_category, rank
);


--
-- Name: idx_ranks_daily_filter; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_ranks_daily_filter ON frontend.store_app_ranks_daily USING btree (
    country, store_collection, store_category, crawled_date
);


--
-- Name: store_app_ranks_daily fk_country; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries (
    id
);


--
-- Name: store_app_ranks_daily fk_store_app; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
ADD CONSTRAINT fk_store_app FOREIGN KEY (
    store_app
) REFERENCES public.store_apps (id);


--
-- Name: store_app_ranks_daily fk_store_category; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
ADD CONSTRAINT fk_store_category FOREIGN KEY (
    store_category
) REFERENCES public.store_categories (id);


--
-- Name: store_app_ranks_daily fk_store_collection; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
ADD CONSTRAINT fk_store_collection FOREIGN KEY (
    store_collection
) REFERENCES public.store_collections (id);


--
-- PostgreSQL database dump complete
--

\unrestrict o5D0NZ65v8XJjCygcWFaDGSw8DoeMbXYmXVOp9TPvf0DtSpQKWjrVC5sz5fLaZU

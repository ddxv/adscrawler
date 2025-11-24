--
-- PostgreSQL database dump
--

\restrict WIaBazbmjmE434K4kmOrU3ni9b6yoDc0AZvlanK9KOt6ytqfH3dep8kklkUxLqW

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
-- Name: store_app_sources; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_sources (
    store smallint NOT NULL,
    store_app integer NOT NULL,
    crawl_source text
);


ALTER TABLE logging.store_app_sources OWNER TO postgres;

--
-- Name: store_app_sources store_app_sources_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_sources
    ADD CONSTRAINT store_app_sources_pk PRIMARY KEY (store, store_app);


--
-- Name: store_app_sources store_app_sources_app_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_sources
    ADD CONSTRAINT store_app_sources_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: store_app_sources store_app_sources_store_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_sources
    ADD CONSTRAINT store_app_sources_store_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- PostgreSQL database dump complete
--

\unrestrict WIaBazbmjmE434K4kmOrU3ni9b6yoDc0AZvlanK9KOt6ytqfH3dep8kklkUxLqW


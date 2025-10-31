--
-- PostgreSQL database dump
--

\restrict sJnfyYrJzNyzy5MT6KnMtV3G4bYR4YTAS8VPrL8qrjocduD9FirGgkGaT3jaa4J

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
-- Name: store_app_country_crawl; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_country_crawl (
    crawl_result smallint,
    store_app integer,
    country_id smallint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.store_app_country_crawl OWNER TO postgres;

--
-- Name: store_app_country_crawl fk_country; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_country_crawl
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: store_app_country_crawl store_app_country_history_app_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_country_crawl
    ADD CONSTRAINT store_app_country_history_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- PostgreSQL database dump complete
--

\unrestrict sJnfyYrJzNyzy5MT6KnMtV3G4bYR4YTAS8VPrL8qrjocduD9FirGgkGaT3jaa4J


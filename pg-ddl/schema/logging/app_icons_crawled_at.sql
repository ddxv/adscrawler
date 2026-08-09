--
-- PostgreSQL database dump
--

\restrict y7Qr3oer7XLPX9XbEYwilQdNvxyjD6jueUhG2g90cBa8Q4BoIgGUpmJeIBSaP5r

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
-- Name: app_icons_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_icons_crawled_at (
    store_app integer NOT NULL,
    crawled_at timestamp without time zone NOT NULL
);


ALTER TABLE logging.app_icons_crawled_at OWNER TO postgres;

--
-- Name: app_icons_crawled_at store_app; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_icons_crawled_at
    ADD CONSTRAINT store_app PRIMARY KEY (store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict y7Qr3oer7XLPX9XbEYwilQdNvxyjD6jueUhG2g90cBa8Q4BoIgGUpmJeIBSaP5r


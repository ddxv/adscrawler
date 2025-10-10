--
-- PostgreSQL database dump
--

\restrict X8r4n2G5ONT2J5gwf4vbThPvF7hPJKdv9a5NyMd5tGMQvVShnbmSrvEs8G5LvlJ

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
-- Name: developers_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.developers_crawled_at (
    developer integer NOT NULL,
    apps_crawled_at timestamp without time zone
);


ALTER TABLE logging.developers_crawled_at OWNER TO postgres;

--
-- Name: developers_crawled_at developers_crawled_at_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.developers_crawled_at
ADD CONSTRAINT developers_crawled_at_pk PRIMARY KEY (developer);


--
-- Name: developers_crawled_at newtable_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.developers_crawled_at
ADD CONSTRAINT newtable_fk FOREIGN KEY (
    developer
) REFERENCES public.developers (id);


--
-- PostgreSQL database dump complete
--

\unrestrict X8r4n2G5ONT2J5gwf4vbThPvF7hPJKdv9a5NyMd5tGMQvVShnbmSrvEs8G5LvlJ

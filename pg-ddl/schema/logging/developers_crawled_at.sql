--
-- PostgreSQL database dump
--

\restrict mcbZRGhP4vHXmWZpOKe59nd7GfwyldH3DhMwAYHnZXzdGsorji8af6cHbnx7Pof

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
    ADD CONSTRAINT newtable_fk FOREIGN KEY (developer) REFERENCES public.developers(id);


--
-- PostgreSQL database dump complete
--

\unrestrict mcbZRGhP4vHXmWZpOKe59nd7GfwyldH3DhMwAYHnZXzdGsorji8af6cHbnx7Pof


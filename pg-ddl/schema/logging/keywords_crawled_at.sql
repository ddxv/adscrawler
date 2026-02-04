--
-- PostgreSQL database dump
--

\restrict RGnoo42s8GJd2T5OZnbJDrMQyNOoX62xzB10KyStOpFcTyJTZZfue502fbsGp8y

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
-- Name: keywords_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.keywords_crawled_at (
    keyword integer NOT NULL,
    crawled_at timestamp without time zone NOT NULL
);


ALTER TABLE logging.keywords_crawled_at OWNER TO postgres;

--
-- Name: keywords_crawled_at keywords_crawled_at_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.keywords_crawled_at
    ADD CONSTRAINT keywords_crawled_at_pk PRIMARY KEY (keyword);


--
-- Name: keywords_crawled_at keywords_crawled_at_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.keywords_crawled_at
    ADD CONSTRAINT keywords_crawled_at_fk FOREIGN KEY (keyword) REFERENCES public.keywords(id);


--
-- PostgreSQL database dump complete
--

\unrestrict RGnoo42s8GJd2T5OZnbJDrMQyNOoX62xzB10KyStOpFcTyJTZZfue502fbsGp8y


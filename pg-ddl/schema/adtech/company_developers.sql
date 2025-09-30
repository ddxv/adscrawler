--
-- PostgreSQL database dump
--

\restrict rwZpLeSjqzM29UuKcMSiTdjPgyG0BE7HYv6Rfw6xFa4HcMgqmFcd8p6egl321e1

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
-- Name: company_developers; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_developers (
    company_id integer NOT NULL,
    developer_id integer NOT NULL
);


ALTER TABLE adtech.company_developers OWNER TO postgres;

--
-- Name: company_developers company_developers_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_developers
ADD CONSTRAINT company_developers_pkey PRIMARY KEY (company_id, developer_id);


--
-- Name: company_developers fk_company_developers_category; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_developers
ADD CONSTRAINT fk_company_developers_category FOREIGN KEY (
    developer_id
) REFERENCES public.developers (id);


--
-- PostgreSQL database dump complete
--

\unrestrict rwZpLeSjqzM29UuKcMSiTdjPgyG0BE7HYv6Rfw6xFa4HcMgqmFcd8p6egl321e1

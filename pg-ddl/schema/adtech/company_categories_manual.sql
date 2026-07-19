--
-- PostgreSQL database dump
--

\restrict ouGlcJHOdqeGPo1aMVIcgycYdvP6rgNSxqQqi5fKzn76tedB3yx4cULkWdatnYF

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: company_categories_manual; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_categories_manual (
    company_id integer NOT NULL,
    category_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE adtech.company_categories_manual OWNER TO postgres;

--
-- Name: company_categories_manual company_categories_manual_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_categories_manual
    ADD CONSTRAINT company_categories_manual_pkey PRIMARY KEY (company_id);


--
-- Name: company_categories_manual company_categories_manual_category_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_categories_manual
    ADD CONSTRAINT company_categories_manual_category_id_fkey FOREIGN KEY (category_id) REFERENCES adtech.categories(id);


--
-- Name: company_categories_manual company_categories_manual_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_categories_manual
    ADD CONSTRAINT company_categories_manual_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id);


--
-- PostgreSQL database dump complete
--

\unrestrict ouGlcJHOdqeGPo1aMVIcgycYdvP6rgNSxqQqi5fKzn76tedB3yx4cULkWdatnYF


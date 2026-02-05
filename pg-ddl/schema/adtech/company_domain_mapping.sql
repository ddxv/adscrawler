--
-- PostgreSQL database dump
--

\restrict 4NKwY4bjB9IJ9dMyQeFED1O5Cbkypstr7L9zxFQgcweH0mbbg2qHYDHz1I9Hpwh

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
-- Name: company_domain_mapping; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_domain_mapping (
    company_id integer NOT NULL,
    domain_id integer NOT NULL
);


ALTER TABLE adtech.company_domain_mapping OWNER TO postgres;

--
-- Name: company_domain_mapping company_domain_mapping_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_domain_mapping
    ADD CONSTRAINT company_domain_mapping_pkey PRIMARY KEY (company_id, domain_id);


--
-- Name: company_domain_mapping company_domain_mapping_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_domain_mapping
    ADD CONSTRAINT company_domain_mapping_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id);


--
-- Name: company_domain_mapping company_domain_mapping_domain_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_domain_mapping
    ADD CONSTRAINT company_domain_mapping_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES public.domains(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict 4NKwY4bjB9IJ9dMyQeFED1O5Cbkypstr7L9zxFQgcweH0mbbg2qHYDHz1I9Hpwh


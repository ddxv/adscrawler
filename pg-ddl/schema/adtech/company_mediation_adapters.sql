--
-- PostgreSQL database dump
--

\restrict zaymzjIzgXchb3cAfb9Yw10WiWTU7jccvvyMVRj8r8i5XrTqEPBT0baAjaKQM68

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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
-- Name: company_mediation_adapters; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_mediation_adapters (
    company_id integer NOT NULL,
    adapter_pattern character varying(100) NOT NULL
);


ALTER TABLE adtech.company_mediation_adapters OWNER TO postgres;

--
-- Name: company_mediation_adapters company_mediation_adapters_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_mediation_adapters
    ADD CONSTRAINT company_mediation_adapters_pkey PRIMARY KEY (company_id, adapter_pattern);


--
-- Name: company_mediation_adapters company_mediation_adapters_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_mediation_adapters
    ADD CONSTRAINT company_mediation_adapters_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id);


--
-- PostgreSQL database dump complete
--

\unrestrict zaymzjIzgXchb3cAfb9Yw10WiWTU7jccvvyMVRj8r8i5XrTqEPBT0baAjaKQM68


--
-- PostgreSQL database dump
--

\restrict VhOif2xEhg3tPNGLg8YGtOY1zaGiEmGZrqqTnma05VyjQpbj9UPIuN5xMPaUbjb

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
-- Name: sdk_mediation_patterns; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_mediation_patterns (
    sdk_id integer NOT NULL,
    mediation_pattern character varying(255) NOT NULL
);


ALTER TABLE adtech.sdk_mediation_patterns OWNER TO postgres;

--
-- Name: sdk_mediation_patterns company_mediation_patterns_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_patterns
    ADD CONSTRAINT company_mediation_patterns_pkey PRIMARY KEY (sdk_id, mediation_pattern);


--
-- Name: sdk_mediation_patterns company_mediation_patterns_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_patterns
    ADD CONSTRAINT company_mediation_patterns_company_id_fkey FOREIGN KEY (sdk_id) REFERENCES adtech.sdks(id);


--
-- PostgreSQL database dump complete
--

\unrestrict VhOif2xEhg3tPNGLg8YGtOY1zaGiEmGZrqqTnma05VyjQpbj9UPIuN5xMPaUbjb


--
-- PostgreSQL database dump
--

\restrict WbwhhMrSkTqA3R7Z6Ag8RA0R3Pw7LoRVdstxV31Afr7hQldzazEg0DjqrXLgH64

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
-- Name: company_country_evidence; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_country_evidence (
    id bigint NOT NULL,
    company_id integer,
    source text NOT NULL,
    raw_value text,
    country_id smallint,
    updated_at timestamp with time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'::text),
    CONSTRAINT company_country_evidence_source_check CHECK ((source = ANY (ARRAY['linkedin'::text, 'github'::text, 'clearbit'::text, 'scraping'::text, 'app_store'::text, 'domain_tld'::text])))
);


ALTER TABLE adtech.company_country_evidence OWNER TO postgres;

--
-- Name: company_country_evidence_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.company_country_evidence ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME adtech.company_country_evidence_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: company_country_evidence company_country_evidence_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_country_evidence
    ADD CONSTRAINT company_country_evidence_pkey PRIMARY KEY (id);


--
-- Name: company_country_evidence unique_company_source_country; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_country_evidence
    ADD CONSTRAINT unique_company_source_country UNIQUE (company_id, source) INCLUDE (country_id);


--
-- Name: company_country_evidence company_country_evidence_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_country_evidence
    ADD CONSTRAINT company_country_evidence_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id) ON DELETE CASCADE;


--
-- Name: company_country_evidence company_country_evidence_country_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_country_evidence
    ADD CONSTRAINT company_country_evidence_country_id_fkey FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- PostgreSQL database dump complete
--

\unrestrict WbwhhMrSkTqA3R7Z6Ag8RA0R3Pw7LoRVdstxV31Afr7hQldzazEg0DjqrXLgH64


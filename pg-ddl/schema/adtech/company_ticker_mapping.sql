--
-- PostgreSQL database dump
--

\restrict 6aDTgLW3YGyEgLnd0A5vrCrM403UpFXqj3NpAcb00cXEhhfBmb5e1cZTO78I1F8

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
-- Name: company_ticker_mapping; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_ticker_mapping (
    company_id bigint NOT NULL,
    ticker_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE adtech.company_ticker_mapping OWNER TO postgres;

--
-- Name: company_ticker_mapping company_ticker_mapping_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_ticker_mapping
    ADD CONSTRAINT company_ticker_mapping_pkey PRIMARY KEY (ticker_id);


--
-- Name: company_ticker_mapping fk_company_ticker_company; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_ticker_mapping
    ADD CONSTRAINT fk_company_ticker_company FOREIGN KEY (company_id) REFERENCES adtech.companies(id) ON DELETE CASCADE;


--
-- Name: company_ticker_mapping fk_company_ticker_ticker; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_ticker_mapping
    ADD CONSTRAINT fk_company_ticker_ticker FOREIGN KEY (ticker_id) REFERENCES adtech.tickers(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict 6aDTgLW3YGyEgLnd0A5vrCrM403UpFXqj3NpAcb00cXEhhfBmb5e1cZTO78I1F8


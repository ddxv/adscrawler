--
-- PostgreSQL database dump
--

\restrict hk2oCPggQvrKmhAGrWwuPcChFEu9ANNPGSVd3DemQXPXJivcp9KXMLMdqpfkRc1

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
-- Name: combined_domain_app_history; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.combined_domain_app_history (
    domain_id bigint NOT NULL,
    store_app bigint NOT NULL,
    sdk boolean NOT NULL,
    api_call boolean NOT NULL,
    app_ads_direct boolean NOT NULL,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    app_ads_reseller boolean DEFAULT false NOT NULL
);


ALTER TABLE adtech.combined_domain_app_history OWNER TO postgres;

--
-- Name: idx_cdah_active_domain_store_year_quarter; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_cdah_active_domain_store_year_quarter ON adtech.combined_domain_app_history USING btree (domain_id, store_app, year, quarter);


--
-- PostgreSQL database dump complete
--

\unrestrict hk2oCPggQvrKmhAGrWwuPcChFEu9ANNPGSVd3DemQXPXJivcp9KXMLMdqpfkRc1


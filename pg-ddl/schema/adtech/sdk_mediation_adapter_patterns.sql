--
-- PostgreSQL database dump
--

\restrict xR2PdrRtysAJO2ZidmIXaNYQNN98IIlun6MC2xErgIldQKpQd4jGQ9QVkyaOMnT

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
-- Name: sdk_mediation_adapter_patterns; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_mediation_adapter_patterns (
    mediation_sdk_id integer NOT NULL,
    adapter_sdk_id integer NOT NULL,
    adapter_pattern text NOT NULL,
    CONSTRAINT sdk_mediation_adapter_patterns_check CHECK ((adapter_sdk_id <> mediation_sdk_id))
);


ALTER TABLE adtech.sdk_mediation_adapter_patterns OWNER TO postgres;

--
-- Name: sdk_mediation_adapter_patterns sdk_mediation_adapter_patterns_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_adapter_patterns
    ADD CONSTRAINT sdk_mediation_adapter_patterns_pkey PRIMARY KEY (adapter_sdk_id, adapter_pattern);


--
-- Name: sdk_mediation_adapter_pattern_lower_uidx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX sdk_mediation_adapter_pattern_lower_uidx ON adtech.sdk_mediation_adapter_patterns USING btree (lower(adapter_pattern));


--
-- Name: sdk_mediation_adapter_patterns sdk_mediation_adapter_patterns_adapter_sdk_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_adapter_patterns
    ADD CONSTRAINT sdk_mediation_adapter_patterns_adapter_sdk_id_fkey FOREIGN KEY (adapter_sdk_id) REFERENCES adtech.sdks(id);


--
-- Name: sdk_mediation_adapter_patterns sdk_mediation_adapter_patterns_mediation_sdk_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_adapter_patterns
    ADD CONSTRAINT sdk_mediation_adapter_patterns_mediation_sdk_id_fkey FOREIGN KEY (mediation_sdk_id) REFERENCES adtech.sdks(id);


--
-- PostgreSQL database dump complete
--

\unrestrict xR2PdrRtysAJO2ZidmIXaNYQNN98IIlun6MC2xErgIldQKpQd4jGQ9QVkyaOMnT


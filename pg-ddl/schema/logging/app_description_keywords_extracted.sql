--
-- PostgreSQL database dump
--

\restrict Ne3VlrceI2bg5sUQMqdi88cX32uV0OOBNfm03smIFAej6uoIsdfJBFvIHGz2VLh

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
-- Name: app_description_keywords_extracted; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_description_keywords_extracted (
    store_app integer NOT NULL,
    description_id integer NOT NULL,
    extracted_at timestamp without time zone
);


ALTER TABLE logging.app_description_keywords_extracted OWNER TO postgres;

--
-- Name: app_description_keywords_extracted app_description_keywords_extracted_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_description_keywords_extracted
    ADD CONSTRAINT app_description_keywords_extracted_pk PRIMARY KEY (store_app, description_id);


--
-- Name: app_description_keywords_extrac_description_id_extracted_at_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX app_description_keywords_extrac_description_id_extracted_at_idx ON logging.app_description_keywords_extracted USING btree (description_id, extracted_at DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict Ne3VlrceI2bg5sUQMqdi88cX32uV0OOBNfm03smIFAej6uoIsdfJBFvIHGz2VLh


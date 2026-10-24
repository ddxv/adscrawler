--
-- PostgreSQL database dump
--

\restrict MexrR80673FMcKBFBZoDtxALn378dSjxikzCUfTwBuMegGTTckzi5RawBtTtHTM

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
-- Name: app_description_keywords_extracted; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_description_keywords_extracted (
    store_app integer NOT NULL,
    description_id integer NOT NULL,
    extracted_at timestamp without time zone
);


ALTER TABLE logging.app_description_keywords_extracted OWNER TO postgres;

--
-- Name: app_description_keywords_extrac_description_id_extracted_at_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX app_description_keywords_extrac_description_id_extracted_at_idx ON logging.app_description_keywords_extracted USING btree (description_id, extracted_at DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict MexrR80673FMcKBFBZoDtxALn378dSjxikzCUfTwBuMegGTTckzi5RawBtTtHTM


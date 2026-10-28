--
-- PostgreSQL database dump
--

\restrict rSkSMK5HNVPP3yEQNeR55T7LDoSrVXszAzQWlco2JTrO3RwjA4kOIiPsnBDR6zg

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
-- Name: s3_reports; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.s3_reports (
    myregion text NOT NULL,
    bucket text NOT NULL,
    report_name text NOT NULL,
    domain_name text NOT NULL,
    file_key text NOT NULL,
    last_modified timestamp with time zone NOT NULL,
    row_count bigint
);


ALTER TABLE public.s3_reports OWNER TO postgres;

--
-- Name: idx_s3_reports_domain_report_file; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_s3_reports_domain_report_file ON public.s3_reports USING btree (domain_name, report_name, file_key);


--
-- Name: s3_reports_domain_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX s3_reports_domain_idx ON public.s3_reports USING btree (domain_name);


--
-- Name: s3_reports_file_key_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX s3_reports_file_key_idx ON public.s3_reports USING btree (myregion, bucket, file_key);


--
-- PostgreSQL database dump complete
--

\unrestrict rSkSMK5HNVPP3yEQNeR55T7LDoSrVXszAzQWlco2JTrO3RwjA4kOIiPsnBDR6zg


--
-- PostgreSQL database dump
--

\restrict S5gKKZ4O67oP9Tb6nui0zzqsWDKP6n4lQ9hyTZFNDN1kOebfzqZEPT5F04ND1SH

-- Dumped from database version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)
-- Dumped by pg_dump version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)

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
-- Name: creative_records; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_records (
    id integer NOT NULL,
    creative_initial_domain_id integer NOT NULL,
    creative_host_domain_id integer NOT NULL,
    run_id integer NOT NULL,
    store_app_pub_id integer NOT NULL,
    creative_asset_id integer NOT NULL,
    updated_at timestamp with time zone DEFAULT now(),
    mmp_domain_id integer,
    mmp_urls text [],
    additional_ad_domain_ids integer []
);


ALTER TABLE public.creative_records OWNER TO postgres;

--
-- Name: creative_records_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_records_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.creative_records_id_seq OWNER TO postgres;

--
-- Name: creative_records_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_records_id_seq OWNED BY public.creative_records.id;


--
-- Name: creative_records id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records ALTER COLUMN id SET DEFAULT nextval(
    'public.creative_records_id_seq'::regclass
);


--
-- Name: creative_records creative_records_creative_initial_domain_id_creative_host_d_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_creative_initial_domain_id_creative_host_d_key UNIQUE (
    creative_initial_domain_id,
    creative_host_domain_id,
    run_id,
    store_app_pub_id,
    creative_asset_id
);


--
-- Name: creative_records creative_records_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_pkey PRIMARY KEY (id);


--
-- Name: creative_records creative_records_creative_host_domain_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_creative_host_domain_id_fkey FOREIGN KEY (
    creative_host_domain_id
) REFERENCES public.ad_domains (id);


--
-- Name: creative_records creative_records_creative_initial_domain_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_creative_initial_domain_id_fkey FOREIGN KEY (
    creative_initial_domain_id
) REFERENCES public.ad_domains (id);


--
-- Name: creative_records creative_records_mmp_domain_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_mmp_domain_id_fkey FOREIGN KEY (
    mmp_domain_id
) REFERENCES public.ad_domains (id);


--
-- Name: creative_records creative_records_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_run_id_fkey FOREIGN KEY (
    run_id
) REFERENCES public.version_code_api_scan_results (id);


--
-- Name: creative_records creative_records_store_app_pub_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
ADD CONSTRAINT creative_records_store_app_pub_id_fkey FOREIGN KEY (
    store_app_pub_id
) REFERENCES public.store_apps (id);


--
-- PostgreSQL database dump complete
--

\unrestrict S5gKKZ4O67oP9Tb6nui0zzqsWDKP6n4lQ9hyTZFNDN1kOebfzqZEPT5F04ND1SH

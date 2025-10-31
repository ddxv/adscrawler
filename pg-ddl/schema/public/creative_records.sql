--
-- PostgreSQL database dump
--

\restrict XhF5y4yOhabVLSjyoeKZsELsWaVbvH4qQV5ojpDV4AMQk2mDEH83WIWwa12fuUE

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
    api_call_id integer NOT NULL,
    creative_asset_id integer NOT NULL,
    creative_host_domain_id integer NOT NULL,
    creative_initial_domain_id integer,
    advertiser_store_app_id integer,
    advertiser_domain_id integer,
    mmp_domain_id integer,
    mmp_urls text[],
    additional_ad_domain_ids integer[],
    created_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    CONSTRAINT check_advertiser_or_advertiser_domain CHECK (((advertiser_store_app_id IS NOT NULL) OR (advertiser_domain_id IS NOT NULL) OR ((advertiser_store_app_id IS NULL) AND (advertiser_domain_id IS NULL))))
);


ALTER TABLE public.creative_records OWNER TO postgres;

--
-- Name: creative_records_id_seq1; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_records_id_seq1
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.creative_records_id_seq1 OWNER TO postgres;

--
-- Name: creative_records_id_seq1; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_records_id_seq1 OWNED BY public.creative_records.id;


--
-- Name: creative_records id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records ALTER COLUMN id SET DEFAULT nextval('public.creative_records_id_seq1'::regclass);


--
-- Name: creative_records creative_records__pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records__pkey PRIMARY KEY (id);


--
-- Name: creative_records creative_records_api_call_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_api_call_id_key UNIQUE (api_call_id);


--
-- Name: creative_records creative_records_advertiser_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_advertiser_app_fk FOREIGN KEY (advertiser_store_app_id) REFERENCES public.store_apps(id);


--
-- Name: creative_records creative_records_advertiser_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_advertiser_domain_fk FOREIGN KEY (advertiser_domain_id) REFERENCES public.domains(id) ON DELETE SET NULL;


--
-- Name: creative_records creative_records_api_call_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_api_call_fk FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id) ON DELETE CASCADE;


--
-- Name: creative_records creative_records_asset_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_asset_fk FOREIGN KEY (creative_asset_id) REFERENCES public.creative_assets(id);


--
-- Name: creative_records creative_records_host_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_host_domain_fk FOREIGN KEY (creative_host_domain_id) REFERENCES public.domains(id) ON DELETE CASCADE;


--
-- Name: creative_records creative_records_initial_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_initial_domain_fk FOREIGN KEY (creative_initial_domain_id) REFERENCES public.domains(id) ON DELETE SET NULL;


--
-- Name: creative_records creative_records_mmp_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_mmp_domain_fk FOREIGN KEY (mmp_domain_id) REFERENCES public.domains(id) ON DELETE SET NULL;


--
-- PostgreSQL database dump complete
--

\unrestrict XhF5y4yOhabVLSjyoeKZsELsWaVbvH4qQV5ojpDV4AMQk2mDEH83WIWwa12fuUE


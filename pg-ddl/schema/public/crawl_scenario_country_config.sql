--
-- PostgreSQL database dump
--

\restrict rg2SWzhFlOdfcpBNEsCV8V1InfYfopcueawxQVIL8ADigME2LV4M66Znlp6n472

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
-- Name: crawl_scenario_country_config; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.crawl_scenario_country_config (
    id integer NOT NULL,
    country_id integer,
    scenario_id integer,
    enabled boolean DEFAULT true,
    priority integer DEFAULT 1,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.crawl_scenario_country_config OWNER TO postgres;

--
-- Name: crawl_scenario_country_config_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.crawl_scenario_country_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.crawl_scenario_country_config_id_seq OWNER TO postgres;

--
-- Name: crawl_scenario_country_config_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.crawl_scenario_country_config_id_seq OWNED BY public.crawl_scenario_country_config.id;


--
-- Name: crawl_scenario_country_config id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config ALTER COLUMN id SET DEFAULT nextval('public.crawl_scenario_country_config_id_seq'::regclass);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_country_id_scenario_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_country_id_scenario_id_key UNIQUE (country_id, scenario_id);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_pkey PRIMARY KEY (id);


--
-- Name: idx_country_crawl_config_lookup; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_country_crawl_config_lookup ON public.crawl_scenario_country_config USING btree (scenario_id, country_id) WHERE (enabled = true);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_country_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_country_id_fkey FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_scenario_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_scenario_id_fkey FOREIGN KEY (scenario_id) REFERENCES public.crawl_scenarios(id);


--
-- PostgreSQL database dump complete
--

\unrestrict rg2SWzhFlOdfcpBNEsCV8V1InfYfopcueawxQVIL8ADigME2LV4M66Znlp6n472


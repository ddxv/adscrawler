--
-- PostgreSQL database dump
--

\restrict LsXFnGfMehdR6jnwzVVfscbKiBE3w3KSqO6rauOizFVaK5ZdEJMcdqKws21V9w9

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
-- Name: ip_geo_snapshots; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ip_geo_snapshots (
    id integer NOT NULL,
    mitm_uuid uuid NOT NULL,
    ip_address inet NOT NULL,
    country_id integer,
    state_iso character varying(4),
    city_name character varying,
    org character varying,
    created_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.ip_geo_snapshots OWNER TO postgres;

--
-- Name: ip_geo_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ip_geo_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ip_geo_snapshots_id_seq OWNER TO postgres;

--
-- Name: ip_geo_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ip_geo_snapshots_id_seq OWNED BY public.ip_geo_snapshots.id;


--
-- Name: ip_geo_snapshots id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots ALTER COLUMN id SET DEFAULT nextval('public.ip_geo_snapshots_id_seq'::regclass);


--
-- Name: ip_geo_snapshots ip_geo_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT ip_geo_snapshots_pkey PRIMARY KEY (id);


--
-- Name: ip_geo_snapshots ip_geo_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT ip_geo_unique_key UNIQUE (mitm_uuid);


--
-- Name: idx_ip_geo_ip_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ip_geo_ip_created ON public.ip_geo_snapshots USING btree (ip_address, created_at DESC);


--
-- Name: idx_ip_geo_mitm_uuid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ip_geo_mitm_uuid ON public.ip_geo_snapshots USING btree (mitm_uuid);


--
-- Name: ip_geo_snapshots fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- PostgreSQL database dump complete
--

\unrestrict LsXFnGfMehdR6jnwzVVfscbKiBE3w3KSqO6rauOizFVaK5ZdEJMcdqKws21V9w9


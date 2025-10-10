--
-- PostgreSQL database dump
--

\restrict VgIllyXzCK5EbGzq6ycMMIUQuWUjmdHNabMPoWnuWUUQ0IKH518S5PtYHaq4ghC

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
-- Name: creative_assets; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_assets (
    id integer NOT NULL,
    md5_hash character varying NOT NULL,
    file_extension character varying NOT NULL,
    phash character varying,
    created_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL
);


ALTER TABLE public.creative_assets OWNER TO postgres;

--
-- Name: creative_assets_new_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_assets_new_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.creative_assets_new_id_seq OWNER TO postgres;

--
-- Name: creative_assets_new_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_assets_new_id_seq OWNED BY public.creative_assets.id;


--
-- Name: creative_assets id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets ALTER COLUMN id SET DEFAULT nextval(
    'public.creative_assets_new_id_seq'::regclass
);


--
-- Name: creative_assets creative_assets_new_md5_hash_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
ADD CONSTRAINT creative_assets_new_md5_hash_key UNIQUE (md5_hash);


--
-- Name: creative_assets creative_assets_new_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
ADD CONSTRAINT creative_assets_new_pkey PRIMARY KEY (id);


--
-- Name: idx_creative_assets_phash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_creative_assets_phash ON public.creative_assets USING btree (
    phash
) WHERE (phash IS NOT null);


--
-- PostgreSQL database dump complete
--

\unrestrict VgIllyXzCK5EbGzq6ycMMIUQuWUjmdHNabMPoWnuWUUQ0IKH518S5PtYHaq4ghC

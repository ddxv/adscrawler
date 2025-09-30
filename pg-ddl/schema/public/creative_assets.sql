--
-- PostgreSQL database dump
--

\restrict BmPAw4GfvrBsaqIX9qqQk4OZ7t6R9bHskfvfSSaKtNP4Gxvk5zq2KD1ary4uTA0

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
    store_app_id integer NOT NULL,
    md5_hash character varying NOT NULL,
    file_extension character varying NOT NULL,
    phash character varying
);


ALTER TABLE public.creative_assets OWNER TO postgres;

--
-- Name: creative_assets_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_assets_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.creative_assets_id_seq OWNER TO postgres;

--
-- Name: creative_assets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_assets_id_seq OWNED BY public.creative_assets.id;


--
-- Name: creative_assets id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets ALTER COLUMN id SET DEFAULT nextval(
    'public.creative_assets_id_seq'::regclass
);


--
-- Name: creative_assets creative_assets_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
ADD CONSTRAINT creative_assets_pkey PRIMARY KEY (id);


--
-- Name: creative_assets creative_assets_store_app_id_md5_hash_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
ADD CONSTRAINT creative_assets_store_app_id_md5_hash_key UNIQUE (
    store_app_id, md5_hash
);


--
-- Name: creative_assets creative_assets_store_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
ADD CONSTRAINT creative_assets_store_app_id_fkey FOREIGN KEY (
    store_app_id
) REFERENCES public.store_apps (id);


--
-- PostgreSQL database dump complete
--

\unrestrict BmPAw4GfvrBsaqIX9qqQk4OZ7t6R9bHskfvfSSaKtNP4Gxvk5zq2KD1ary4uTA0

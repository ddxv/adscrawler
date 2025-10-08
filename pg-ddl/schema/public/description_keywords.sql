--
-- PostgreSQL database dump
--

\restrict yxvKunaXspk0cw4oFIDtP2FDGariYj2M7MyuecNudVOzA2cYNdZtoecpPgwNpWd

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
-- Name: description_keywords; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.description_keywords (
    id integer NOT NULL,
    description_id integer NOT NULL,
    keyword_id integer NOT NULL,
    extracted_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.description_keywords OWNER TO postgres;

--
-- Name: description_keywords_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.description_keywords_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.description_keywords_id_seq OWNER TO postgres;

--
-- Name: description_keywords_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.description_keywords_id_seq OWNED BY public.description_keywords.id;


--
-- Name: description_keywords id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords ALTER COLUMN id SET DEFAULT nextval(
    'public.description_keywords_id_seq'::regclass
);


--
-- Name: description_keywords description_keywords_description_id_keyword_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
ADD CONSTRAINT description_keywords_description_id_keyword_id_key UNIQUE (
    description_id, keyword_id
);


--
-- Name: description_keywords description_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
ADD CONSTRAINT description_keywords_pkey PRIMARY KEY (id);


--
-- Name: description_keywords description_keywords_description_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
ADD CONSTRAINT description_keywords_description_id_fkey FOREIGN KEY (
    description_id
) REFERENCES public.store_apps_descriptions (id) ON DELETE CASCADE;


--
-- Name: description_keywords description_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
ADD CONSTRAINT description_keywords_keyword_id_fkey FOREIGN KEY (
    keyword_id
) REFERENCES public.keywords (id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict yxvKunaXspk0cw4oFIDtP2FDGariYj2M7MyuecNudVOzA2cYNdZtoecpPgwNpWd

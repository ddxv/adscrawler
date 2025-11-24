--
-- PostgreSQL database dump
--

\restrict qEoufOWPNchE8RW5MTZISbXNTKXdhI5r0A5KbvzbQn9yO4JfOvA9Y4NUkj0M9yL

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
-- Name: user_requested_scan; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_requested_scan (
    id integer NOT NULL,
    store_id character varying NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.user_requested_scan OWNER TO postgres;

--
-- Name: user_requested_scan_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.user_requested_scan_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.user_requested_scan_id_seq OWNER TO postgres;

--
-- Name: user_requested_scan_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.user_requested_scan_id_seq OWNED BY public.user_requested_scan.id;


--
-- Name: user_requested_scan id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_requested_scan ALTER COLUMN id SET DEFAULT nextval('public.user_requested_scan_id_seq'::regclass);


--
-- Name: user_requested_scan user_requested_scan_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_requested_scan
    ADD CONSTRAINT user_requested_scan_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict qEoufOWPNchE8RW5MTZISbXNTKXdhI5r0A5KbvzbQn9yO4JfOvA9Y4NUkj0M9yL


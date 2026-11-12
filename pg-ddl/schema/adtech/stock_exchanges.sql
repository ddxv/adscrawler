--
-- PostgreSQL database dump
--

\restrict gL0MAGcnqFgxe2ZgpDVCwsLXUidmspj3Fl4NT9I86yjh9vhggjcyblkENZQFzMv

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
-- Name: stock_exchanges; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.stock_exchanges (
    id integer NOT NULL,
    exchange_code character varying(20) NOT NULL,
    exchange_name character varying(150) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE adtech.stock_exchanges OWNER TO postgres;

--
-- Name: stock_exchanges_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.stock_exchanges ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME adtech.stock_exchanges_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: stock_exchanges stock_exchanges_exchange_code_key; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.stock_exchanges
    ADD CONSTRAINT stock_exchanges_exchange_code_key UNIQUE (exchange_code);


--
-- Name: stock_exchanges stock_exchanges_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.stock_exchanges
    ADD CONSTRAINT stock_exchanges_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict gL0MAGcnqFgxe2ZgpDVCwsLXUidmspj3Fl4NT9I86yjh9vhggjcyblkENZQFzMv


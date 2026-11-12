--
-- PostgreSQL database dump
--

\restrict hoYdJ9gUo50WTWGLV7ZF6ffi2tO4SaVZYfsqJ9cr12BTBC0EQHgKrXHCwJPNY9n

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
-- Name: tickers; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.tickers (
    id integer NOT NULL,
    symbol character varying(32) NOT NULL,
    exchange_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    ticker_company_name text
);


ALTER TABLE adtech.tickers OWNER TO postgres;

--
-- Name: tickers_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.tickers ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME adtech.tickers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tickers tickers_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.tickers
    ADD CONSTRAINT tickers_pkey PRIMARY KEY (id);


--
-- Name: tickers uq_tickers_symbol_exchange; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.tickers
    ADD CONSTRAINT uq_tickers_symbol_exchange UNIQUE (symbol, exchange_id);


--
-- Name: tickers fk_tickers_exchange; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.tickers
    ADD CONSTRAINT fk_tickers_exchange FOREIGN KEY (exchange_id) REFERENCES adtech.stock_exchanges(id);


--
-- PostgreSQL database dump complete
--

\unrestrict hoYdJ9gUo50WTWGLV7ZF6ffi2tO4SaVZYfsqJ9cr12BTBC0EQHgKrXHCwJPNY9n


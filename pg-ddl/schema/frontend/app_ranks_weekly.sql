--
-- PostgreSQL database dump
--

\restrict qIZQzySMpoSNLo9PZZF23ilKD2HiUlhvg28AuIgPa3R1gaUpJmfISrZMYLs0hQi

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

--
-- Name: app_ranks_weekly; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.app_ranks_weekly (
    rank smallint NOT NULL,
    best_rank smallint NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    crawled_date date NOT NULL,
    store_app integer NOT NULL
)
PARTITION BY RANGE (crawled_date);


ALTER TABLE frontend.app_ranks_weekly OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict qIZQzySMpoSNLo9PZZF23ilKD2HiUlhvg28AuIgPa3R1gaUpJmfISrZMYLs0hQi


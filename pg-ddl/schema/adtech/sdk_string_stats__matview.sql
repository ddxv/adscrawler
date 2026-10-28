--
-- PostgreSQL database dump
--

\restrict SHxCWzHZYhZLvnccepTYJyvz0dFF3cGQbz1G4smHeN9yhfK9JT7DWrdi3kLiXwz

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
-- Name: sdk_string_stats; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.sdk_string_stats AS
 SELECT string_id,
    sdk_id,
    count(*) AS app_count
   FROM adtech.app_sdk_strings ass
  GROUP BY string_id, sdk_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.sdk_string_stats OWNER TO postgres;

--
-- Name: sdk_string_stats_uniq_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX sdk_string_stats_uniq_idx ON adtech.sdk_string_stats USING btree (string_id, sdk_id);


--
-- PostgreSQL database dump complete
--

\unrestrict SHxCWzHZYhZLvnccepTYJyvz0dFF3cGQbz1G4smHeN9yhfK9JT7DWrdi3kLiXwz


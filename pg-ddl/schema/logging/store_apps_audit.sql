--
-- PostgreSQL database dump
--

\restrict 7QO7A7FKXtsfEv8FXNSXcV5HNcX4dNmmRoAx6kaVf0kbm427Bj2mLvRHUnp8Yjq

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
-- Name: store_apps_audit; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_audit (
    operation character(1) NOT NULL,
    stamp timestamp without time zone NOT NULL,
    userid text NOT NULL,
    row_id bigint NOT NULL,
    store smallint NOT NULL,
    store_id text NOT NULL,
    crawl_result integer
);


ALTER TABLE logging.store_apps_audit OWNER TO postgres;

--
-- Name: store_apps_audit_stamp_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX store_apps_audit_stamp_idx ON logging.store_apps_audit USING btree (stamp);


--
-- PostgreSQL database dump complete
--

\unrestrict 7QO7A7FKXtsfEv8FXNSXcV5HNcX4dNmmRoAx6kaVf0kbm427Bj2mLvRHUnp8Yjq


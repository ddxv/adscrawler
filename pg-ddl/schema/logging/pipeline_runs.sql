--
-- PostgreSQL database dump
--

\restrict eW1MFZJcJUIq707T9sPxLiiGoEniI7r3hz850ZthuAA0GDhZNwxB3pugBPiDzMU

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
-- Name: pipeline_runs; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.pipeline_runs (
    id bigint NOT NULL,
    parent_run_id bigint,
    pipeline_name text NOT NULL,
    step_name text,
    unit_key text DEFAULT ''::text NOT NULL,
    attempt integer DEFAULT 1 NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    heartbeat_at timestamp with time zone,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_summary text,
    CONSTRAINT pipeline_runs_attempt_check CHECK ((attempt > 0)),
    CONSTRAINT pipeline_runs_check CHECK ((((parent_run_id IS NULL) AND (step_name IS NULL)) OR ((parent_run_id IS NOT NULL) AND (step_name IS NOT NULL)))),
    CONSTRAINT pipeline_runs_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'completed'::text, 'partial_failure'::text, 'failed'::text, 'skipped'::text])))
);


ALTER TABLE logging.pipeline_runs OWNER TO postgres;

--
-- Name: pipeline_runs_id_seq; Type: SEQUENCE; Schema: logging; Owner: postgres
--

ALTER TABLE logging.pipeline_runs ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME logging.pipeline_runs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: pipeline_runs pipeline_runs_pkey; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (id);


--
-- Name: pipeline_runs_latest_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX pipeline_runs_latest_idx ON logging.pipeline_runs USING btree (pipeline_name, created_at DESC) WHERE (parent_run_id IS NULL);


--
-- Name: pipeline_runs_step_attempt_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE UNIQUE INDEX pipeline_runs_step_attempt_idx ON logging.pipeline_runs USING btree (parent_run_id, step_name, unit_key, attempt) WHERE (parent_run_id IS NOT NULL);


--
-- Name: pipeline_runs pipeline_runs_parent_run_id_fkey; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.pipeline_runs
    ADD CONSTRAINT pipeline_runs_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES logging.pipeline_runs(id);


--
-- PostgreSQL database dump complete
--

\unrestrict eW1MFZJcJUIq707T9sPxLiiGoEniI7r3hz850ZthuAA0GDhZNwxB3pugBPiDzMU


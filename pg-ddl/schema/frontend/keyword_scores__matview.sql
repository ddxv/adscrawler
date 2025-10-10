--
-- PostgreSQL database dump
--

\restrict IKkiqfgMZGFimQarpAM7LAt8uNmD9cMoamUTMIFZtah3vWtMVRuc0Pid6JlXLhv

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
-- Name: keyword_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.keyword_scores AS
 WITH latest_en_descriptions AS (
         SELECT DISTINCT ON (sad.store_app) sad.store_app,
            sad.id AS description_id
           FROM (public.store_apps_descriptions sad
             JOIN public.description_keywords dk ON ((sad.id = dk.description_id)))
          WHERE (sad.language_id = 1)
          ORDER BY sad.store_app, sad.updated_at DESC
        ), keyword_app_counts AS (
         SELECT sa.store,
            k.keyword_text,
            dk.keyword_id,
            count(DISTINCT led.store_app) AS app_count
           FROM (((latest_en_descriptions led
             LEFT JOIN public.description_keywords dk ON ((led.description_id = dk.description_id)))
             LEFT JOIN public.keywords k ON ((dk.keyword_id = k.id)))
             LEFT JOIN public.store_apps sa ON ((led.store_app = sa.id)))
          WHERE (dk.keyword_id IS NOT NULL)
          GROUP BY sa.store, k.keyword_text, dk.keyword_id
        ), total_app_count AS (
         SELECT sa.store,
            count(*) AS total_apps
           FROM (latest_en_descriptions led
             LEFT JOIN public.store_apps sa ON ((led.store_app = sa.id)))
          GROUP BY sa.store
        )
 SELECT kac.store,
    kac.keyword_text,
    kac.keyword_id,
    kac.app_count,
    tac.total_apps,
    round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2) AS competitiveness_score
   FROM (keyword_app_counts kac
     LEFT JOIN total_app_count tac ON ((kac.store = tac.store)))
  ORDER BY (round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.keyword_scores OWNER TO postgres;

--
-- Name: keyword_scores_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX keyword_scores_unique ON frontend.keyword_scores USING btree (store, keyword_id);


--
-- PostgreSQL database dump complete
--

\unrestrict IKkiqfgMZGFimQarpAM7LAt8uNmD9cMoamUTMIFZtah3vWtMVRuc0Pid6JlXLhv


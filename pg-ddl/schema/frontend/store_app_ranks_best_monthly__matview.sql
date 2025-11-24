--
-- PostgreSQL database dump
--

\restrict j2Q6JLTGm0fBTUJhtXfc43HqI3AvTIM6ZD4fasxIrfRSjl6bu5q3t2gGxaBFYeD

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
-- Name: store_app_ranks_best_monthly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_ranks_best_monthly AS
 WITH overview_ranks AS (
         SELECT sa.store_id,
            c.alpha2 AS country,
            sc.collection,
            sca.category,
            min(sarw.best_rank) AS best_rank
           FROM ((((frontend.store_app_ranks_weekly sarw
             LEFT JOIN public.store_apps sa ON ((sarw.store_app = sa.id)))
             LEFT JOIN public.store_collections sc ON ((sarw.store_collection = sc.id)))
             LEFT JOIN public.store_categories sca ON ((sarw.store_category = sca.id)))
             LEFT JOIN public.countries c ON ((sarw.country = c.id)))
          WHERE (sarw.crawled_date >= (CURRENT_DATE - '30 days'::interval))
          GROUP BY sa.store_id, c.alpha2, sc.collection, sca.category
        )
 SELECT store_id,
    country,
    collection,
    category,
    best_rank
   FROM overview_ranks
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_ranks_best_monthly OWNER TO postgres;

--
-- Name: idx_store_app_ranks_best_monthly_store; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_store_app_ranks_best_monthly_store ON frontend.store_app_ranks_best_monthly USING btree (store_id);


--
-- Name: store_app_ranks_best_monthly_uidx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_app_ranks_best_monthly_uidx ON frontend.store_app_ranks_best_monthly USING btree (store_id, country, collection, category);


--
-- PostgreSQL database dump complete
--

\unrestrict j2Q6JLTGm0fBTUJhtXfc43HqI3AvTIM6ZD4fasxIrfRSjl6bu5q3t2gGxaBFYeD


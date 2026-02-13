--
-- PostgreSQL database dump
--

\restrict XaL7BcyRBVAt4FwL7eQKnzFlyjGyEKtl0L6eHaapHWeHMgZjkKIuAMaTlBcetgR

-- Dumped from database version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)
-- Dumped by pg_dump version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)

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
 WITH keyword_app_counts AS (
         SELECT sa.store,
            k.keyword_text,
            ake.keyword_id,
            count(DISTINCT ake.store_app) AS app_count,
            array_length(string_to_array((k.keyword_text)::text, ' '::text), 1) AS word_count
           FROM ((public.app_keywords_extracted ake
             LEFT JOIN public.keywords k ON ((ake.keyword_id = k.id)))
             LEFT JOIN public.store_apps sa ON ((ake.store_app = sa.id)))
          GROUP BY sa.store, k.keyword_text, ake.keyword_id
        ), total_app_count AS (
         SELECT sa.store,
            count(DISTINCT ake.store_app) AS total_apps
           FROM (public.app_keywords_extracted ake
             LEFT JOIN public.store_apps sa ON ((ake.store_app = sa.id)))
          GROUP BY sa.store
        ), keyword_competitors AS (
         SELECT ake.keyword_id,
            sa.store,
            avg(COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25))) AS avg_installs,
            max(COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25))) AS max_installs,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25)))::double precision)) AS median_installs,
            avg(agml.rating) AS avg_rating,
            count(*) FILTER (WHERE (COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25)) > 1000000)) AS apps_over_1m_installs,
            count(*) FILTER (WHERE ((sa.name)::text ~~* (('%'::text || (k.keyword_text)::text) || '%'::text))) AS title_matches
           FROM (((public.app_keywords_extracted ake
             LEFT JOIN public.store_apps sa ON ((ake.store_app = sa.id)))
             LEFT JOIN public.app_global_metrics_latest agml ON ((sa.id = agml.store_app)))
             LEFT JOIN public.keywords k ON ((ake.keyword_id = k.id)))
          GROUP BY ake.keyword_id, sa.store, k.keyword_text
        ), keyword_metrics AS (
         SELECT kac.store,
            kac.keyword_text,
            kac.keyword_id,
            kac.app_count,
            round(kc.avg_installs, 0) AS avg_installs,
            tac.total_apps,
            round(((100.0 * (kac.app_count)::numeric) / (NULLIF(tac.total_apps, 0))::numeric), 2) AS market_penetration_pct,
            round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2) AS competitiveness_score,
            kac.word_count,
                CASE
                    WHEN (kac.word_count = 1) THEN 'short_tail'::text
                    WHEN (kac.word_count = 2) THEN 'medium_tail'::text
                    ELSE 'long_tail'::text
                END AS keyword_type,
            length((kac.keyword_text)::text) AS char_length,
            (COALESCE(kc.avg_installs, (0)::numeric))::bigint AS avg_competitor_installs,
            COALESCE(kc.max_installs, (0)::bigint) AS top_competitor_installs,
            (COALESCE(kc.median_installs, (0)::double precision))::bigint AS median_competitor_installs,
            COALESCE(kc.avg_rating, (0)::double precision) AS avg_competitor_rating,
            COALESCE(kc.apps_over_1m_installs, (0)::bigint) AS major_competitors,
            COALESCE(kc.title_matches, (0)::bigint) AS title_matches,
            round(((100.0 * (COALESCE(kc.title_matches, (0)::bigint))::numeric) / (NULLIF(kac.app_count, 0))::numeric), 2) AS title_relevance_pct
           FROM ((keyword_app_counts kac
             LEFT JOIN total_app_count tac ON ((kac.store = tac.store)))
             LEFT JOIN keyword_competitors kc ON (((kac.keyword_id = kc.keyword_id) AND (kac.store = kc.store))))
        )
 SELECT store,
    keyword_text,
    keyword_id,
    app_count,
    avg_installs,
    total_apps,
    market_penetration_pct,
    competitiveness_score,
    word_count,
    keyword_type,
    char_length,
    avg_competitor_installs,
    top_competitor_installs,
    median_competitor_installs,
    avg_competitor_rating,
    major_competitors,
    title_matches,
    title_relevance_pct,
    round(LEAST((100)::numeric, ((((app_count)::numeric * 10.0) * ((100)::numeric - competitiveness_score)) / 100.0)), 2) AS volume_competition_score,
    round(LEAST((100)::numeric, ((competitiveness_score * 0.6) + (LEAST((100)::numeric, ((COALESCE(avg_competitor_installs, (0)::bigint))::numeric / 100000.0)) * 0.4))), 2) AS keyword_difficulty,
    round(
        CASE
            WHEN (app_count < 10) THEN (0)::double precision
            WHEN ((major_competitors)::numeric > ((app_count)::numeric * 0.25)) THEN (20)::double precision
            ELSE ((LEAST((40)::double precision, (log(((app_count + 1))::double precision) * (10)::double precision)) + ((((100)::numeric - competitiveness_score) * 0.4))::double precision) + (
            CASE
                WHEN (COALESCE(median_competitor_installs, (0)::bigint) < 100000) THEN 20
                WHEN (COALESCE(median_competitor_installs, (0)::bigint) < 1000000) THEN 15
                WHEN (COALESCE(median_competitor_installs, (0)::bigint) < 10000000) THEN 10
                ELSE 5
            END)::double precision)
        END) AS opportunity_score,
        CASE
            WHEN (app_count > 0) THEN round(((((app_count)::numeric * 1000.0) * (1.0 / ((1)::numeric + (competitiveness_score / 50.0)))) *
            CASE
                WHEN (word_count = 1) THEN 2.0
                WHEN (word_count = 2) THEN 1.0
                ELSE 0.5
            END), 0)
            ELSE (0)::numeric
        END AS estimated_monthly_searches,
    round(((100)::numeric - LEAST((100)::numeric, ((((major_competitors)::numeric * 10.0) + ((COALESCE(median_competitor_installs, (0)::bigint))::numeric / 100000.0)) + (competitiveness_score * 0.3)))), 2) AS ranking_feasibility
   FROM keyword_metrics km
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.keyword_scores OWNER TO postgres;

--
-- Name: keyword_scores_store_keyword_id_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX keyword_scores_store_keyword_id_idx ON frontend.keyword_scores USING btree (store, keyword_id);


--
-- PostgreSQL database dump complete
--

\unrestrict XaL7BcyRBVAt4FwL7eQKnzFlyjGyEKtl0L6eHaapHWeHMgZjkKIuAMaTlBcetgR


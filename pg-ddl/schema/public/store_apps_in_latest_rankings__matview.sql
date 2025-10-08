--
-- PostgreSQL database dump
--

\restrict CJ6UHYFFiFU4A0Ec5MN1wyJtfCuYndH9rLnv5wge2L9NxduGqmi92tiyIpcAZo5

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
-- Name: store_apps_in_latest_rankings; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_in_latest_rankings AS
WITH growth_apps AS (
    SELECT
        sa.id AS store_app,
        sa.store,
        sa.store_last_updated,
        sa.name,
        sa.installs,
        sa.rating_count,
        sa.store_id
    FROM (
        frontend.store_apps_z_scores AS saz
        LEFT JOIN
            public.store_apps AS sa
            ON (((saz.store_id)::text = (sa.store_id)::text))
    )
    WHERE sa.free
    ORDER BY COALESCE(saz.installs_z_score_2w, saz.ratings_z_score_2w) DESC
    LIMIT 500
), ranked_apps AS (
    SELECT DISTINCT ON (ar.store_app)
        ar.store_app,
        sa.store,
        sa.store_last_updated,
        sa.name,
        sa.installs,
        sa.rating_count,
        sa.store_id
    FROM ((
        frontend.store_app_ranks_weekly ar
        LEFT JOIN public.store_apps AS sa ON ((ar.store_app = sa.id))
    )
    LEFT JOIN public.countries AS c ON ((ar.country = c.id))
    )
    WHERE
        (
            sa.free
            AND (ar.store_collection = ANY(ARRAY[1, 3, 4, 6]))
            AND (ar.crawled_date > (CURRENT_DATE - '15 days'::interval))
            AND (
                (c.alpha2)::text
                = ANY(
                    ARRAY[
                        ('US'::character varying)::text,
                        ('GB'::character varying)::text,
                        ('CA'::character varying)::text,
                        ('AR'::character varying)::text,
                        ('CN'::character varying)::text,
                        ('DE'::character varying)::text,
                        ('ID'::character varying)::text,
                        ('IN'::character varying)::text,
                        ('JP'::character varying)::text,
                        ('FR'::character varying)::text,
                        ('BR'::character varying)::text,
                        ('MX'::character varying)::text,
                        ('KR'::character varying)::text,
                        ('RU'::character varying)::text
                    ]
                )
            )
            AND (ar.rank < 150)
        )
)
SELECT
    growth_apps.store_app,
    growth_apps.store,
    growth_apps.store_last_updated,
    growth_apps.name,
    growth_apps.installs,
    growth_apps.rating_count,
    growth_apps.store_id
FROM growth_apps
UNION
SELECT
    ranked_apps.store_app,
    ranked_apps.store,
    ranked_apps.store_last_updated,
    ranked_apps.name,
    ranked_apps.installs,
    ranked_apps.rating_count,
    ranked_apps.store_id
FROM ranked_apps
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_in_latest_rankings OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict CJ6UHYFFiFU4A0Ec5MN1wyJtfCuYndH9rLnv5wge2L9NxduGqmi92tiyIpcAZo5

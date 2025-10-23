--
-- PostgreSQL database dump
--

\restrict nFFIcYflKyircclwq7pljCJeVAfEuEWo39DnJy98iNr5q1JzQj1nuPat07gnfga

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
-- Name: companies_parent_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_parent_category_stats AS
WITH d30_counts AS (
    SELECT
        sahw.store_app,
        sum(sahw.installs_diff) AS d30_installs,
        sum(sahw.rating_count_diff) AS d30_rating_count
    FROM public.store_apps_history_weekly AS sahw
    WHERE
        (
            (sahw.week_start > (current_date - '31 days'::interval))
            AND (sahw.country_id = 840)
            AND (
                (sahw.installs_diff > (0)::numeric)
                OR (sahw.rating_count_diff > 0)
            )
        )
    GROUP BY sahw.store_app
), distinct_apps_group AS (
    SELECT
        sa.store,
        csac.store_app,
        csac.app_category,
        c.name AS company_name,
        sa.installs,
        sa.rating_count,
        coalesce(ad.domain_name, csac.ad_domain) AS company_domain
    FROM (((
        adtech.combined_store_apps_companies csac
        LEFT JOIN adtech.companies AS c ON ((csac.parent_id = c.id))
    )
    LEFT JOIN public.domains AS ad ON ((c.domain_id = ad.id))
    )
    LEFT JOIN public.store_apps AS sa ON ((csac.store_app = sa.id))
    )
    WHERE (csac.parent_id IN (
        SELECT DISTINCT pc.id
        FROM (
            adtech.companies AS pc
            LEFT JOIN
                adtech.companies AS c_1
                ON ((pc.id = c_1.parent_company_id))
        )
        WHERE (c_1.id IS NOT null)
    ))
)
SELECT
    dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
FROM (
    distinct_apps_group AS dag
    LEFT JOIN d30_counts AS dc ON ((dag.store_app = dc.store_app))
)
GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_parent_category_stats OWNER TO postgres;

--
-- Name: companies_parent_category_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_parent_category_stats_idx ON frontend.companies_parent_category_stats USING btree (
    store, company_domain, company_name, app_category
);


--
-- Name: companies_parent_category_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_parent_category_stats_query_idx ON frontend.companies_parent_category_stats USING btree (
    company_domain
);


--
-- PostgreSQL database dump complete
--

\unrestrict nFFIcYflKyircclwq7pljCJeVAfEuEWo39DnJy98iNr5q1JzQj1nuPat07gnfga

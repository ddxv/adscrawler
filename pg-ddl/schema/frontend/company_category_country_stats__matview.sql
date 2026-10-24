--
-- PostgreSQL database dump
--

\restrict G8M2dPfaUgGHpRvTqlsqH8wNdfhEgDv8VMzP1CSZTIf8XsqGzUKwNW2PdejdLJf

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
-- Name: company_category_country_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_category_country_stats AS
 WITH target_apps AS (
         SELECT cpca.store_app,
            cpca.company_id,
            cpca.sdk,
            cpca.api_call,
            cpca.publisher,
            cpca.app_ads_direct,
            cpca.app_ads_reseller
           FROM adtech.combined_parent_company_apps cpca
          WHERE ((cpca.sdk OR cpca.api_call OR cpca.app_ads_direct OR cpca.publisher) AND (cpca.company_id IN ( SELECT cpca_1.company_id
                   FROM adtech.combined_parent_company_apps cpca_1
                  WHERE (cpca_1.sdk OR cpca_1.api_call OR cpca_1.app_ads_direct OR cpca_1.publisher)
                  GROUP BY cpca_1.company_id
                 HAVING (count(*) > 100))))
        ), company_app_countries AS (
         SELECT ta.company_id,
            sa.store,
            sa.category AS app_category,
            sa.country_id,
            count(*) AS app_count
           FROM (target_apps ta
             JOIN frontend.store_apps_overview sa ON ((ta.store_app = sa.id)))
          GROUP BY ta.company_id, sa.store, sa.category, sa.country_id
        )
 SELECT d.domain_name,
    cac.store,
    cac.app_category,
    cc.alpha2 AS country,
    cac.app_count
   FROM (((company_app_countries cac
     JOIN adtech.companies c ON ((c.id = cac.company_id)))
     JOIN public.domains d ON ((c.domain_id = d.id)))
     JOIN public.countries cc ON ((cc.id = cac.country_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_category_country_stats OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict G8M2dPfaUgGHpRvTqlsqH8wNdfhEgDv8VMzP1CSZTIf8XsqGzUKwNW2PdejdLJf


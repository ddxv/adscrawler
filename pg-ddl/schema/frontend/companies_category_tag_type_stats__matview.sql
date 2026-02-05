--
-- PostgreSQL database dump
--

\restrict b1ttbPFZXMG8nIliCUxvtljeGDzTWGrRgKzOsVZ6VU5I4qxhrpbEMZ34CI4pBUz

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
-- Name: companies_category_tag_type_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_tag_type_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), minimized_company_categories AS (
         SELECT company_categories.company_id,
            min(company_categories.category_id) AS category_id
           FROM adtech.company_categories
          GROUP BY company_categories.company_id
        ), api_and_app_ads AS (
         SELECT sa.store,
            csac.app_category,
            tag.tag_source,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
                CASE
                    WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
                    ELSE cats.url_slug
                END AS type_url_slug,
            count(DISTINCT csac.store_app) AS app_count,
            sum(dc.d30_installs) AS installs_d30,
            sum(dc.d30_rating_count) AS rating_count_d30,
            sum(sa.installs) AS installs_total,
            sum(sa.rating_count) AS rating_count_total
           FROM ((((((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
             LEFT JOIN d30_counts dc ON ((csac.store_app = dc.store_app)))
             LEFT JOIN minimized_company_categories mcc ON ((csac.company_id = mcc.company_id)))
             LEFT JOIN adtech.categories cats ON ((mcc.category_id = cats.id)))
             CROSS JOIN LATERAL ( VALUES ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
          GROUP BY sa.store, csac.app_category, tag.tag_source, csac.ad_domain, c.name,
                CASE
                    WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
                    ELSE cats.url_slug
                END
        ), store_app_sdks AS (
         SELECT DISTINCT sass.store_app,
            sass.sdk_id
           FROM adtech.store_app_sdk_strings sass
          WHERE (sass.sdk_id IS NOT NULL)
        ), sdk_and_mediation AS (
         SELECT sa.store,
            sa.category AS app_category,
            'sdk'::text AS tag_source,
            d.domain_name AS company_domain,
            c.name AS company_name,
            cats.url_slug AS type_url_slug,
            count(DISTINCT sas.store_app) AS app_count,
            sum(dc.d30_installs) AS installs_d30,
            sum(dc.d30_rating_count) AS rating_count_d30,
            sum(sa.installs) AS installs_total,
            sum(sa.rating_count) AS rating_count_total
           FROM (((((((store_app_sdks sas
             LEFT JOIN adtech.sdks s ON ((sas.sdk_id = s.id)))
             LEFT JOIN adtech.companies c ON ((s.company_id = c.id)))
             LEFT JOIN public.domains d ON ((c.domain_id = d.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((sas.store_app = sa.id)))
             LEFT JOIN d30_counts dc ON ((sas.store_app = dc.store_app)))
             LEFT JOIN adtech.sdk_categories sc ON ((sas.sdk_id = sc.sdk_id)))
             LEFT JOIN adtech.categories cats ON ((sc.category_id = cats.id)))
          GROUP BY sa.store, sa.category, 'sdk'::text, d.domain_name, c.name, cats.url_slug
        )
 SELECT api_and_app_ads.store,
    api_and_app_ads.app_category,
    api_and_app_ads.tag_source,
    api_and_app_ads.company_domain,
    api_and_app_ads.company_name,
    api_and_app_ads.type_url_slug,
    api_and_app_ads.app_count,
    api_and_app_ads.installs_d30,
    api_and_app_ads.rating_count_d30,
    api_and_app_ads.installs_total,
    api_and_app_ads.rating_count_total
   FROM api_and_app_ads
UNION ALL
 SELECT sdk_and_mediation.store,
    sdk_and_mediation.app_category,
    sdk_and_mediation.tag_source,
    sdk_and_mediation.company_domain,
    sdk_and_mediation.company_name,
    sdk_and_mediation.type_url_slug,
    sdk_and_mediation.app_count,
    sdk_and_mediation.installs_d30,
    sdk_and_mediation.rating_count_d30,
    sdk_and_mediation.installs_total,
    sdk_and_mediation.rating_count_total
   FROM sdk_and_mediation
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_type_stats OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict b1ttbPFZXMG8nIliCUxvtljeGDzTWGrRgKzOsVZ6VU5I4qxhrpbEMZ34CI4pBUz


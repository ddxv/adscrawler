--
-- PostgreSQL database dump
--

\restrict BDYVeqQAbV5Wag6iNnxaOseoJR6TxFmv1rzFBaCwfAfbYR8FWu5AdhA2cBQb4yY

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
-- Name: category_tag_type_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.category_tag_type_stats AS
 WITH minimized_company_categories AS (
         SELECT company_categories.company_id,
            min(company_categories.category_id) AS category_id
           FROM adtech.company_categories
          GROUP BY company_categories.company_id
        ), api_and_app_ads AS (
         SELECT x.store,
            x.app_category,
            x.tag_source,
            x.type_url_slug,
            count(*) AS app_count,
            sum(x.installs_sum_4w_est) AS installs_d30,
            sum(x.installs_est) AS installs_total
           FROM ( SELECT DISTINCT csac.store_app,
                    sa.store,
                    csac.app_category,
                    tag.tag_source,
                        CASE
                            WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
                            ELSE cats.url_slug
                        END AS type_url_slug,
                    sa.installs_sum_4w_est,
                    sa.installs_est
                   FROM ((((adtech.combined_store_apps_companies csac
                     LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
                     JOIN minimized_company_categories mcc ON ((csac.company_id = mcc.company_id)))
                     LEFT JOIN adtech.categories cats ON ((mcc.category_id = cats.id)))
                     CROSS JOIN LATERAL ( VALUES ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
                  WHERE ((tag.present IS TRUE) AND (sa.id IS NOT NULL))) x
          GROUP BY x.store, x.app_category, x.tag_source, x.type_url_slug
        ), store_app_sdks AS (
         SELECT DISTINCT sass.store_app,
            sass.sdk_id
           FROM adtech.store_app_sdk_strings sass
          WHERE (sass.sdk_id IS NOT NULL)
        ), sdk_and_mediation AS (
         SELECT x.store,
            x.app_category,
            'sdk'::text AS tag_source,
            x.type_url_slug,
            count(*) AS app_count,
            sum(x.installs_sum_4w_est) AS installs_d30,
            sum(x.installs_est) AS installs_total
           FROM ( SELECT DISTINCT sas.store_app,
                    sa.store,
                    sa.category AS app_category,
                    cats.url_slug AS type_url_slug,
                    sa.installs_sum_4w_est,
                    sa.installs_est
                   FROM (((store_app_sdks sas
                     LEFT JOIN frontend.store_apps_overview sa ON ((sas.store_app = sa.id)))
                     LEFT JOIN adtech.sdk_categories sc ON ((sas.sdk_id = sc.sdk_id)))
                     LEFT JOIN adtech.categories cats ON ((sc.category_id = cats.id)))
                  WHERE (sa.id IS NOT NULL)) x
          GROUP BY x.store, x.app_category, x.type_url_slug
        )
 SELECT api_and_app_ads.store,
    api_and_app_ads.app_category,
    api_and_app_ads.tag_source,
    api_and_app_ads.type_url_slug,
    api_and_app_ads.app_count,
    api_and_app_ads.installs_d30,
    api_and_app_ads.installs_total
   FROM api_and_app_ads
UNION ALL
 SELECT sdk_and_mediation.store,
    sdk_and_mediation.app_category,
    sdk_and_mediation.tag_source,
    sdk_and_mediation.type_url_slug,
    sdk_and_mediation.app_count,
    sdk_and_mediation.installs_d30,
    sdk_and_mediation.installs_total
   FROM sdk_and_mediation
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.category_tag_type_stats OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict BDYVeqQAbV5Wag6iNnxaOseoJR6TxFmv1rzFBaCwfAfbYR8FWu5AdhA2cBQb4yY


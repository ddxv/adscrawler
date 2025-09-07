--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        )
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
     LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
     LEFT JOIN d30_counts dc ON ((csac.store_app = dc.store_app)))
     LEFT JOIN adtech.company_categories ccats ON ((csac.company_id = ccats.company_id)))
     LEFT JOIN adtech.categories cats ON ((ccats.category_id = cats.id)))
     CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
  WHERE (tag.present IS TRUE)
  GROUP BY sa.store, csac.app_category, tag.tag_source, csac.ad_domain, c.name,
        CASE
            WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
            ELSE cats.url_slug
        END
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_type_stats OWNER TO postgres;

--
-- Name: companies_category_tag_type_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_category_tag_type_stats_idx ON frontend.companies_category_tag_type_stats USING btree (store, tag_source, app_category, company_domain, type_url_slug);


--
-- Name: companies_category_tag_type_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_category_tag_type_stats_query_idx ON frontend.companies_category_tag_type_stats USING btree (type_url_slug, app_category);


--
-- PostgreSQL database dump complete
--


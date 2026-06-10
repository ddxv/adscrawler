--
-- PostgreSQL database dump
--

\restrict Hg4gaLYbQEdCiJ2rahu8N8dbKODkCgg4PNKlsMMD79ESjRM3HEYFiroOnjLYcQ8

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: companies_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_overview AS
 WITH domain_base AS (
         SELECT d.id AS domain_id,
            d.domain_name AS company_domain,
            cac.company_id,
            c_1.name AS company_name,
            c_1.logo_url,
            c_1.parent_company_id,
            pc_1.name AS parent_company_name,
            pd.domain_name AS parent_domain,
            pd.id AS parent_domain_id,
            bool_or(cac.sdk) AS has_sdk_signal,
            bool_or(cac.api_call) AS has_api_signal,
            bool_or(cac.publisher) AS has_publisher_signal,
            bool_or(cac.app_ads_direct) AS has_app_ads_direct,
            bool_or(cac.app_ads_reseller) AS has_app_ads_reseller
           FROM ((((adtech.combined_app_companies cac
             JOIN public.domains d ON ((cac.domain_id = d.id)))
             LEFT JOIN adtech.companies c_1 ON ((cac.company_id = c_1.id)))
             LEFT JOIN adtech.companies pc_1 ON ((c_1.parent_company_id = pc_1.id)))
             LEFT JOIN public.domains pd ON ((pc_1.domain_id = pd.id)))
          GROUP BY d.id, d.domain_name, cac.company_id, c_1.name, c_1.logo_url, c_1.parent_company_id, pc_1.name, pd.domain_name, pd.id
        ), creatives_agg AS (
         SELECT companies_creative_rankings.company_domain,
            (count(*))::integer AS creatives_app_count
           FROM frontend.companies_creative_rankings
          WHERE (companies_creative_rankings.last_seen < (now() - '1 day'::interval))
          GROUP BY companies_creative_rankings.company_domain
        ), trends_agg AS (
         SELECT DISTINCT TRIM(BOTH FROM lower((sub.company_domain)::text)) AS company_domain,
            1 AS has_trends
           FROM ( SELECT trend_companies.company_domain
                   FROM adtech.trend_companies
                  WHERE ((trend_companies.year > 2025) OR ((trend_companies.year = 2025) AND (trend_companies.quarter >= 2)))
                UNION
                 SELECT trend_domains.domain_name
                   FROM adtech.trend_domains
                  WHERE ((trend_domains.year > 2025) OR ((trend_domains.year = 2025) AND (trend_domains.quarter >= 2)))
                UNION
                 SELECT trend_parent_companies.company_domain
                   FROM adtech.trend_parent_companies
                  WHERE ((trend_parent_companies.year > 2025) OR ((trend_parent_companies.year = 2025) AND (trend_parent_companies.quarter >= 2)))) sub
        ), app_changes_agg AS (
         SELECT d.domain_name AS company_domain,
            (count(*) FILTER (WHERE (dacq.status = 'added'::adtech.change_status_enum)))::integer AS apps_added_count,
            (count(*) FILTER (WHERE (dacq.status = 'removed'::adtech.change_status_enum)))::integer AS apps_lost_count
           FROM (adtech.domain_app_changes_quarterly dacq
             LEFT JOIN public.domains d ON ((dacq.domain_id = d.id)))
          GROUP BY d.domain_name
        ), sdks_agg AS (
         SELECT sub.company_domain,
            (max(sub.sdk_count))::integer AS sdk_count
           FROM ( SELECT companies_sdks_overview.company_domain,
                    count(DISTINCT companies_sdks_overview.sdk_name) AS sdk_count
                   FROM frontend.companies_sdks_overview
                  GROUP BY companies_sdks_overview.company_domain
                UNION ALL
                 SELECT companies_sdks_overview.parent_company_domain,
                    count(DISTINCT companies_sdks_overview.sdk_name) AS sdk_count
                   FROM frontend.companies_sdks_overview
                  WHERE (companies_sdks_overview.parent_company_domain IS NOT NULL)
                  GROUP BY companies_sdks_overview.parent_company_domain) sub
          GROUP BY sub.company_domain
        ), mediation_agg AS (
         SELECT mediation_adapter_app_counts.mediation_domain AS company_domain,
            (count(DISTINCT mediation_adapter_app_counts.adapter_domain))::integer AS mediation_adapter_count
           FROM frontend.mediation_adapter_app_counts
          GROUP BY mediation_adapter_app_counts.mediation_domain
        ), adstxt_ad_domain_agg AS (
         SELECT adstxt_ad_domain_overview.ad_domain_url,
            sum(adstxt_ad_domain_overview.app_count) AS adstxt_direct_app_count
           FROM frontend.adstxt_ad_domain_overview
          WHERE ((adstxt_ad_domain_overview.relationship)::text = 'DIRECT'::text)
          GROUP BY adstxt_ad_domain_overview.ad_domain_url
        )
 SELECT dom.company_domain,
    dom.domain_id,
    dom.company_id,
    dom.company_name,
    dom.logo_url,
    dom.parent_company_id,
    dom.parent_domain,
    dom.parent_domain_id,
    dom.has_sdk_signal,
    dom.has_api_signal,
    dom.has_publisher_signal,
    dom.has_app_ads_direct,
    dom.has_app_ads_reseller,
    COALESCE(c.creatives_app_count, pc.creatives_app_count, 0) AS creatives_app_count,
    COALESCE(t.has_trends, pt.has_trends, 0) AS has_trends,
    COALESCE(a.apps_added_count, pa.apps_added_count, 0) AS apps_added_count,
    COALESCE(a.apps_lost_count, pa.apps_lost_count, 0) AS apps_lost_count,
    COALESCE(s.sdk_count, ps.sdk_count, 0) AS sdk_count,
    COALESCE(m.mediation_adapter_count, pm.mediation_adapter_count, 0) AS mediation_adapter_count,
    COALESCE(c.creatives_app_count, 0) AS creatives_app_count_direct,
    COALESCE(t.has_trends, 0) AS has_trends_direct,
    COALESCE(a.apps_added_count, 0) AS apps_added_count_direct,
    COALESCE(a.apps_lost_count, 0) AS apps_lost_count_direct,
    COALESCE(s.sdk_count, 0) AS sdk_count_direct,
    COALESCE(m.mediation_adapter_count, 0) AS mediation_adapter_count_direct,
    COALESCE(aa.adstxt_direct_app_count, (0)::numeric) AS adstxt_direct_app_count,
    (((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
           FROM adtech.companies
          WHERE (companies.parent_company_id IS NOT NULL)))))::integer AS is_parent_domain
   FROM ((((((((((((domain_base dom
     LEFT JOIN creatives_agg c ON (((dom.company_domain)::text = (c.company_domain)::text)))
     LEFT JOIN trends_agg t ON (((dom.company_domain)::text = t.company_domain)))
     LEFT JOIN app_changes_agg a ON (((dom.company_domain)::text = (a.company_domain)::text)))
     LEFT JOIN sdks_agg s ON (((dom.company_domain)::text = (s.company_domain)::text)))
     LEFT JOIN mediation_agg m ON (((dom.company_domain)::text = (m.company_domain)::text)))
     LEFT JOIN adstxt_ad_domain_agg aa ON (((dom.company_domain)::text = (aa.ad_domain_url)::text)))
     LEFT JOIN creatives_agg pc ON (((dom.parent_domain)::text = (pc.company_domain)::text)))
     LEFT JOIN trends_agg pt ON (((dom.parent_domain)::text = pt.company_domain)))
     LEFT JOIN app_changes_agg pa ON (((dom.parent_domain)::text = (pa.company_domain)::text)))
     LEFT JOIN sdks_agg ps ON (((dom.parent_domain)::text = (ps.company_domain)::text)))
     LEFT JOIN mediation_agg pm ON (((dom.parent_domain)::text = (pm.company_domain)::text)))
     LEFT JOIN adstxt_ad_domain_agg paa ON (((dom.parent_domain)::text = (paa.ad_domain_url)::text)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_overview OWNER TO postgres;

--
-- Name: frontend_companies_overview; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX frontend_companies_overview ON frontend.companies_overview USING btree (domain_id);


--
-- Name: frontend_companies_overview_domain; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX frontend_companies_overview_domain ON frontend.companies_overview USING btree (company_domain);


--
-- PostgreSQL database dump complete
--

\unrestrict Hg4gaLYbQEdCiJ2rahu8N8dbKODkCgg4PNKlsMMD79ESjRM3HEYFiroOnjLYcQ8


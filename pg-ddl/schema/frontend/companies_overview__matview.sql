--
-- PostgreSQL database dump
--

\restrict qf7FZrfxavz8P8VghU7m2f3hF2YTCxICrvQeS86rVDmQ1geGLv0dPLfpG0jTYqD

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
            c_1.linkedin_url,
            c_1.github_user,
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
          GROUP BY d.id, d.domain_name, cac.company_id, c_1.name, c_1.logo_url, c_1.linkedin_url, c_1.github_user, c_1.parent_company_id, pc_1.name, pd.domain_name, pd.id
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
                  WHERE (ROW(trend_companies.year, trend_companies.quarter) >= ROW((EXTRACT(year FROM (CURRENT_DATE - '3 mons'::interval)))::integer, (EXTRACT(quarter FROM (CURRENT_DATE - '3 mons'::interval)))::integer))
                UNION
                 SELECT trend_domains.domain_name
                   FROM adtech.trend_domains
                  WHERE (ROW(trend_domains.year, trend_domains.quarter) >= ROW((EXTRACT(year FROM (CURRENT_DATE - '3 mons'::interval)))::integer, (EXTRACT(quarter FROM (CURRENT_DATE - '3 mons'::interval)))::integer))
                UNION
                 SELECT trend_parent_companies.company_domain
                   FROM adtech.trend_parent_companies
                  WHERE (ROW(trend_parent_companies.year, trend_parent_companies.quarter) >= ROW((EXTRACT(year FROM (CURRENT_DATE - '3 mons'::interval)))::integer, (EXTRACT(quarter FROM (CURRENT_DATE - '3 mons'::interval)))::integer))) sub
        ), app_changes_agg AS (
         SELECT d.domain_name AS company_domain,
            (count(*) FILTER (WHERE ((dacq.status = 'added'::adtech.change_status_enum) AND (dacq.tag_source = 'sdk'::text))))::integer AS apps_sdk_added_count,
            (count(*) FILTER (WHERE ((dacq.status = 'removed'::adtech.change_status_enum) AND (dacq.tag_source = 'sdk'::text))))::integer AS apps_sdk_lost_count,
            (count(*) FILTER (WHERE ((dacq.status = 'added'::adtech.change_status_enum) AND (dacq.tag_source = 'app_ads_direct'::text))))::integer AS apps_adstxt_direct_added_count,
            (count(*) FILTER (WHERE ((dacq.status = 'removed'::adtech.change_status_enum) AND (dacq.tag_source = 'app_ads_direct'::text))))::integer AS apps_adstxt_direct_lost_count
           FROM (adtech.domain_app_changes_quarterly dacq
             LEFT JOIN public.domains d ON ((dacq.domain_id = d.id)))
          WHERE (ROW(dacq.year, dacq.quarter) >= ROW((EXTRACT(year FROM (CURRENT_DATE - '3 mons'::interval)))::integer, (EXTRACT(quarter FROM (CURRENT_DATE - '3 mons'::interval)))::integer))
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
        ), parent_adstxt_ad_domain_agg AS (
         SELECT aadpo.ad_domain_url,
            sum(aadpo.app_count) AS adstxt_parent_app_count
           FROM frontend.adstxt_ad_domain_parent_overview aadpo
          WHERE ((aadpo.relationship)::text = 'DIRECT'::text)
          GROUP BY aadpo.ad_domain_url
        ), ip_country_resolved AS (
         SELECT company_domain_country.company_domain,
            company_domain_country.most_common_country AS api_ip_resolved_country
           FROM frontend.company_domain_country
        ), percent_opensource AS (
         SELECT companies_open_source_percent.company_domain,
            companies_open_source_percent.percent_open_source
           FROM frontend.companies_open_source_percent
        ), country_resolved AS (
         SELECT e.company_id,
            c_1.alpha2 AS country
           FROM (( SELECT company_country_evidence.company_id,
                    company_country_evidence.country_id,
                    row_number() OVER (PARTITION BY company_country_evidence.company_id ORDER BY
                        CASE company_country_evidence.source
                            WHEN 'manual'::text THEN 1
                            WHEN 'linkedin'::text THEN 2
                            WHEN 'domain_tld'::text THEN 3
                            WHEN 'app_store'::text THEN 4
                            ELSE 5
                        END, company_country_evidence.updated_at DESC) AS priority_rank
                   FROM adtech.company_country_evidence
                  WHERE (company_country_evidence.country_id IS NOT NULL)) e
             JOIN public.countries c_1 ON ((e.country_id = c_1.id)))
          WHERE (e.priority_rank = 1)
        ), parent_creatives_rollup AS (
         SELECT db.parent_domain,
            (sum(c_1.creatives_app_count))::integer AS total_count
           FROM (domain_base db
             JOIN creatives_agg c_1 ON (((db.company_domain)::text = (c_1.company_domain)::text)))
          WHERE (db.parent_domain IS NOT NULL)
          GROUP BY db.parent_domain
        ), parent_changes_rollup AS (
         SELECT db.parent_domain,
            (sum(a_1.apps_sdk_added_count))::integer AS apps_sdk_added_count,
            (sum(a_1.apps_sdk_lost_count))::integer AS apps_sdk_lost_count,
            (sum(a_1.apps_adstxt_direct_added_count))::integer AS apps_adstxt_direct_added_count,
            (sum(a_1.apps_adstxt_direct_lost_count))::integer AS apps_adstxt_direct_lost_count
           FROM (domain_base db
             JOIN app_changes_agg a_1 ON (((db.company_domain)::text = (a_1.company_domain)::text)))
          WHERE (db.parent_domain IS NOT NULL)
          GROUP BY db.parent_domain
        ), parent_sdks_rollup AS (
         SELECT db.parent_domain,
            max(s_1.sdk_count) AS max_count
           FROM (domain_base db
             JOIN sdks_agg s_1 ON (((db.company_domain)::text = (s_1.company_domain)::text)))
          WHERE (db.parent_domain IS NOT NULL)
          GROUP BY db.parent_domain
        ), parent_mediation_rollup AS (
         SELECT db.parent_domain,
            (sum(m_1.mediation_adapter_count))::integer AS total_count
           FROM (domain_base db
             JOIN mediation_agg m_1 ON (((db.company_domain)::text = (m_1.company_domain)::text)))
          WHERE (db.parent_domain IS NOT NULL)
          GROUP BY db.parent_domain
        )
 SELECT dom.company_domain,
    dom.domain_id,
    dom.company_id,
    dom.company_name,
    dom.logo_url,
    dom.linkedin_url,
    dom.github_user,
    dom.parent_company_id,
    dom.parent_domain,
    dom.parent_domain_id,
    dom.has_sdk_signal,
    dom.has_api_signal,
    dom.has_publisher_signal,
    dom.has_app_ads_direct,
    dom.has_app_ads_reseller,
    COALESCE(co.country, pco.country) AS country,
    co.country AS country_direct,
    COALESCE(ipco.api_ip_resolved_country, pipco.api_ip_resolved_country) AS api_ip_resolved_country,
    ipco.api_ip_resolved_country AS api_ip_resolved_country_direct,
    COALESCE(po.percent_open_source, ppo.percent_open_source) AS percent_open_source,
    po.percent_open_source AS percent_open_source_direct,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN (COALESCE(c.creatives_app_count, 0) + COALESCE(p_cr.total_count, 0))
            ELSE COALESCE(c.creatives_app_count, 0)
        END AS creatives_app_count,
    COALESCE(t.has_trends, pt.has_trends, 0) AS has_trends,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN (COALESCE(a.apps_sdk_added_count, 0) + COALESCE(p_ch.apps_sdk_added_count, 0))
            ELSE COALESCE(a.apps_sdk_added_count, 0)
        END AS apps_sdk_added_count,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN (COALESCE(a.apps_sdk_lost_count, 0) + COALESCE(p_ch.apps_sdk_lost_count, 0))
            ELSE COALESCE(a.apps_sdk_lost_count, 0)
        END AS apps_sdk_lost_count,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN (COALESCE(a.apps_adstxt_direct_added_count, 0) + COALESCE(p_ch.apps_adstxt_direct_added_count, 0))
            ELSE COALESCE(a.apps_adstxt_direct_added_count, 0)
        END AS apps_adstxt_direct_added_count,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN (COALESCE(a.apps_adstxt_direct_lost_count, 0) + COALESCE(p_ch.apps_adstxt_direct_lost_count, 0))
            ELSE COALESCE(a.apps_adstxt_direct_lost_count, 0)
        END AS apps_adstxt_direct_lost_count,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN GREATEST(COALESCE(s.sdk_count, 0), COALESCE(p_sd.max_count, 0))
            ELSE COALESCE(s.sdk_count, 0)
        END AS sdk_count,
        CASE
            WHEN ((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
               FROM adtech.companies
              WHERE (companies.parent_company_id IS NOT NULL)))) THEN (COALESCE(m.mediation_adapter_count, 0) + COALESCE(p_me.total_count, 0))
            ELSE COALESCE(m.mediation_adapter_count, 0)
        END AS mediation_adapter_count,
    COALESCE(c.creatives_app_count, 0) AS creatives_app_count_direct,
    COALESCE(t.has_trends, 0) AS has_trends_direct,
    COALESCE(a.apps_sdk_added_count, 0) AS apps_sdk_added_count_direct,
    COALESCE(a.apps_sdk_lost_count, 0) AS apps_sdk_lost_count_direct,
    COALESCE(a.apps_adstxt_direct_added_count, 0) AS apps_adstxt_direct_added_count_direct,
    COALESCE(a.apps_adstxt_direct_lost_count, 0) AS apps_adstxt_direct_lost_count_direct,
    COALESCE(s.sdk_count, 0) AS sdk_count_direct,
    COALESCE(m.mediation_adapter_count, 0) AS mediation_adapter_count_direct,
    COALESCE(aa.adstxt_direct_app_count, (0)::numeric) AS adstxt_direct_app_count,
    COALESCE(paa.adstxt_parent_app_count, (0)::numeric) AS adstxt_parent_app_count,
    (((dom.company_id IS NOT NULL) AND (dom.company_id IN ( SELECT DISTINCT companies.parent_company_id
           FROM adtech.companies
          WHERE (companies.parent_company_id IS NOT NULL)))))::integer AS is_parent_domain
   FROM ((((((((((((((((((domain_base dom
     LEFT JOIN creatives_agg c ON (((dom.company_domain)::text = (c.company_domain)::text)))
     LEFT JOIN trends_agg t ON (((dom.company_domain)::text = t.company_domain)))
     LEFT JOIN app_changes_agg a ON (((dom.company_domain)::text = (a.company_domain)::text)))
     LEFT JOIN sdks_agg s ON (((dom.company_domain)::text = (s.company_domain)::text)))
     LEFT JOIN mediation_agg m ON (((dom.company_domain)::text = (m.company_domain)::text)))
     LEFT JOIN adstxt_ad_domain_agg aa ON (((dom.company_domain)::text = (aa.ad_domain_url)::text)))
     LEFT JOIN parent_adstxt_ad_domain_agg paa ON (((dom.company_domain)::text = (paa.ad_domain_url)::text)))
     LEFT JOIN parent_creatives_rollup p_cr ON (((dom.company_domain)::text = (p_cr.parent_domain)::text)))
     LEFT JOIN parent_changes_rollup p_ch ON (((dom.company_domain)::text = (p_ch.parent_domain)::text)))
     LEFT JOIN parent_sdks_rollup p_sd ON (((dom.company_domain)::text = (p_sd.parent_domain)::text)))
     LEFT JOIN parent_mediation_rollup p_me ON (((dom.company_domain)::text = (p_me.parent_domain)::text)))
     LEFT JOIN trends_agg pt ON (((dom.parent_domain)::text = pt.company_domain)))
     LEFT JOIN country_resolved co ON ((dom.company_id = co.company_id)))
     LEFT JOIN country_resolved pco ON ((dom.parent_company_id = pco.company_id)))
     LEFT JOIN ip_country_resolved ipco ON (((dom.company_domain)::text = (ipco.company_domain)::text)))
     LEFT JOIN ip_country_resolved pipco ON (((dom.parent_domain)::text = (pipco.company_domain)::text)))
     LEFT JOIN percent_opensource po ON (((dom.company_domain)::text = (po.company_domain)::text)))
     LEFT JOIN percent_opensource ppo ON (((dom.parent_domain)::text = (ppo.company_domain)::text)))
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

\unrestrict qf7FZrfxavz8P8VghU7m2f3hF2YTCxICrvQeS86rVDmQ1geGLv0dPLfpG0jTYqD


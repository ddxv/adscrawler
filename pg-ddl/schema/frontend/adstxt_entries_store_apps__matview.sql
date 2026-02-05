--
-- PostgreSQL database dump
--

\restrict yliGCpdPivcsrqmOEPrWV67L6nuhonMli2eKzkkP1W6xXj0Rfvdf0fO4DObSLNO

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
-- Name: adstxt_entries_store_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_entries_store_apps AS
 WITH parent_companies AS (
         SELECT c.id AS company_id,
            c.name AS company_name,
            COALESCE(c.parent_company_id, c.id) AS parent_company_id,
            COALESCE(pc.name, c.name) AS parent_company_name,
            COALESCE(pc.domain_id, c.domain_id) AS parent_company_domain_id
           FROM (adtech.companies c
             LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
        )
 SELECT DISTINCT ad.id AS ad_domain_id,
    myc.parent_company_id AS company_id,
    aae.id AS app_ad_entry_id,
    sa.id AS store_app,
    pd.id AS pub_domain_id
   FROM (((((((public.app_ads_entrys aae
     LEFT JOIN public.domains ad ON ((aae.ad_domain = ad.id)))
     LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
     LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
     LEFT JOIN public.app_urls_map aum ON ((pd.id = aum.pub_domain)))
     JOIN public.store_apps sa ON ((aum.store_app = sa.id)))
     LEFT JOIN parent_companies myc ON ((ad.id = myc.parent_company_domain_id)))
     LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
  WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_entries_store_apps OWNER TO postgres;

--
-- Name: adstxt_entries_store_apps_domain_pub_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_entries_store_apps_domain_pub_idx ON frontend.adstxt_entries_store_apps USING btree (ad_domain_id, app_ad_entry_id);


--
-- Name: adstxt_entries_store_apps_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_entries_store_apps_idx ON frontend.adstxt_entries_store_apps USING btree (store_app);


--
-- Name: adstxt_entries_store_apps_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_entries_store_apps_unique_idx ON frontend.adstxt_entries_store_apps USING btree (ad_domain_id, app_ad_entry_id, store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict yliGCpdPivcsrqmOEPrWV67L6nuhonMli2eKzkkP1W6xXj0Rfvdf0fO4DObSLNO


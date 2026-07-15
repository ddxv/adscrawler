--
-- PostgreSQL database dump
--

\restrict INvp2xMHGWVgwdu797RojN7Ncec16TeXtaxLzUoMydcFXyj1EtEevOzF3kids2A

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
-- Name: adstxt_ad_domain_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_ad_domain_overview AS
 SELECT ad.domain_name AS ad_domain_url,
    aae.relationship,
    sa.store,
    count(DISTINCT aae.publisher_id) AS publisher_id_count,
    count(DISTINCT sa.developer) AS developer_count,
    count(DISTINCT aum.store_app) AS app_count
   FROM ((((frontend.adstxt_domain_entries aesa
     LEFT JOIN public.app_urls_map aum ON ((aesa.pub_domain_id = aum.pub_domain)))
     LEFT JOIN public.store_apps sa ON ((aum.store_app = sa.id)))
     LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
     LEFT JOIN public.domains ad ON ((aesa.ad_domain_id = ad.id)))
  GROUP BY ad.domain_name, aae.relationship, sa.store
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_ad_domain_overview OWNER TO postgres;

--
-- Name: adstxt_ad_domain_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_ad_domain_overview_idx ON frontend.adstxt_ad_domain_overview USING btree (ad_domain_url);


--
-- Name: adstxt_ad_domain_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_ad_domain_overview_unique_idx ON frontend.adstxt_ad_domain_overview USING btree (ad_domain_url, relationship, store);


--
-- PostgreSQL database dump complete
--

\unrestrict INvp2xMHGWVgwdu797RojN7Ncec16TeXtaxLzUoMydcFXyj1EtEevOzF3kids2A


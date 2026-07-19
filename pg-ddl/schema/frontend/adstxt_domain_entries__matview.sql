--
-- PostgreSQL database dump
--

\restrict SnMJq8SPHIhZMif8mZldXtKGhdMgRWmf0teTArefj9tdlSHhd5bfI3QgFwJ6vtn

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
-- Name: adstxt_domain_entries; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_domain_entries AS
 SELECT DISTINCT aae.ad_domain AS ad_domain_id,
    aae.id AS app_ad_entry_id,
    aam.pub_domain AS pub_domain_id
   FROM ((public.app_ads_entrys aae
     JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
     LEFT JOIN public.adstxt_crawl_results pdcr ON ((aam.pub_domain = pdcr.domain_id)))
  WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_domain_entries OWNER TO postgres;

--
-- Name: adstxt_domain_entries_ad_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_domain_entries_ad_idx ON frontend.adstxt_domain_entries USING btree (ad_domain_id, app_ad_entry_id);


--
-- Name: adstxt_domain_entries_pub_domain_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_domain_entries_pub_domain_idx ON frontend.adstxt_domain_entries USING btree (pub_domain_id);


--
-- Name: adstxt_domain_entries_uniq; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_domain_entries_uniq ON frontend.adstxt_domain_entries USING btree (ad_domain_id, app_ad_entry_id, pub_domain_id);


--
-- PostgreSQL database dump complete
--

\unrestrict SnMJq8SPHIhZMif8mZldXtKGhdMgRWmf0teTArefj9tdlSHhd5bfI3QgFwJ6vtn


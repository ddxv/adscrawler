--
-- PostgreSQL database dump
--

\restrict Yu8MJkDhYuOjDQd7axo8f7hqI7OTkzI4Qh92PfmJ5CQWCgcO5khTo9jFceOHTWi

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
-- Name: adstxt_publishers_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_publishers_overview AS
 WITH ranked_data AS (
         SELECT ad.domain_name AS ad_domain_url,
            aae.relationship,
            sa.store,
            aae.publisher_id,
            count(DISTINCT sa.developer) AS developer_count,
            count(DISTINCT aesa.store_app) AS app_count,
            row_number() OVER (PARTITION BY ad.domain_name, aae.relationship, sa.store ORDER BY (count(DISTINCT aesa.store_app)) DESC) AS pubrank
           FROM (((frontend.adstxt_entries_store_apps aesa
             LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
             LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
             LEFT JOIN public.domains ad ON ((aesa.ad_domain_id = ad.id)))
          GROUP BY ad.domain_name, aae.relationship, sa.store, aae.publisher_id
        )
 SELECT ad_domain_url,
    relationship,
    store,
    publisher_id,
    developer_count,
    app_count,
    pubrank
   FROM ranked_data
  WHERE (pubrank <= 50)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_publishers_overview OWNER TO postgres;

--
-- Name: adstxt_publishers_overview_ad_domain_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_publishers_overview_ad_domain_idx ON frontend.adstxt_publishers_overview USING btree (ad_domain_url);


--
-- Name: adstxt_publishers_overview_ad_domain_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_publishers_overview_ad_domain_unique_idx ON frontend.adstxt_publishers_overview USING btree (ad_domain_url, relationship, store, publisher_id);


--
-- PostgreSQL database dump complete
--

\unrestrict Yu8MJkDhYuOjDQd7axo8f7hqI7OTkzI4Qh92PfmJ5CQWCgcO5khTo9jFceOHTWi


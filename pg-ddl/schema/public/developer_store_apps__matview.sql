--
-- PostgreSQL database dump
--

\restrict ktyHtxXW2zqnXoVrohSBA1G8pgbanDnB2VjUDrck4NeNWK0oxxzado8OSQNfy5p

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
-- Name: developer_store_apps; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.developer_store_apps AS
 WITH developer_domain_ids AS (
         SELECT DISTINCT pd_1.id AS domain_id
           FROM (((public.app_urls_map aum_1
             LEFT JOIN public.domains pd_1 ON ((aum_1.pub_domain = pd_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((aum_1.store_app = sa_1.id)))
             LEFT JOIN public.developers d_1 ON ((sa_1.developer = d_1.id)))
        )
 SELECT sa.store,
    sa.id AS store_app,
    d.name AS developer_name,
    pd.domain_name AS developer_url,
    d.store AS developer_store,
    pd.id AS domain_id,
    d.developer_id
   FROM (((public.store_apps sa
     LEFT JOIN public.developers d ON ((sa.developer = d.id)))
     LEFT JOIN public.app_urls_map aum ON ((sa.id = aum.store_app)))
     LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.developer_store_apps OWNER TO postgres;

--
-- Name: developer_store_apps_query; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX developer_store_apps_query ON public.developer_store_apps USING btree (developer_id);


--
-- Name: idx_developer_store_apps_developer_domain; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_developer_store_apps_developer_domain ON public.developer_store_apps USING btree (developer_id, domain_id);


--
-- Name: idx_developer_store_apps_domain_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_developer_store_apps_domain_id ON public.developer_store_apps USING btree (domain_id);


--
-- Name: idx_developer_store_apps_unique; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_developer_store_apps_unique ON public.developer_store_apps USING btree (developer_id, store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict ktyHtxXW2zqnXoVrohSBA1G8pgbanDnB2VjUDrck4NeNWK0oxxzado8OSQNfy5p


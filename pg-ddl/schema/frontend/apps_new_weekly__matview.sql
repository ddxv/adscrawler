--
-- PostgreSQL database dump
--

\restrict 1Ichnu339lmboR2IUTNEA27XnIqwwL0LhypdddfZIfDKpnQTJXYaLKZ4S5IYhJb

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
-- Name: apps_new_weekly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_weekly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
            sa_1.developer_name,
            sa_1.rating,
            sa_1.installs,
            sa_1.installs_sum_1w,
            sa_1.installs_sum_4w,
            sa_1.ratings_sum_1w,
            sa_1.ratings_sum_4w,
            sa_1.store_last_updated,
            sa_1.ad_supported,
            sa_1.in_app_purchases,
            sa_1.created_at,
            sa_1.updated_at,
            sa_1.crawl_result,
            sa_1.icon_url_100,
            sa_1.release_date,
            sa_1.rating_count,
            sa_1.featured_image_url,
            sa_1.phone_image_url_1,
            sa_1.phone_image_url_2,
            sa_1.phone_image_url_3,
            sa_1.tablet_image_url_1,
            sa_1.tablet_image_url_2,
            sa_1.tablet_image_url_3,
            row_number() OVER (PARTITION BY sa_1.store, sa_1.category ORDER BY sa_1.installs DESC NULLS LAST, sa_1.rating_count DESC NULLS LAST) AS rn
           FROM frontend.store_apps_overview sa_1
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '7 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '11 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT id,
    name,
    store_id,
    store,
    category,
    developer_name,
    rating,
    installs,
    installs_sum_1w,
    installs_sum_4w,
    ratings_sum_1w,
    ratings_sum_4w,
    store_last_updated,
    ad_supported,
    in_app_purchases,
    created_at,
    updated_at,
    crawl_result,
    icon_url_100,
    release_date,
    rating_count,
    featured_image_url,
    phone_image_url_1,
    phone_image_url_2,
    phone_image_url_3,
    tablet_image_url_1,
    tablet_image_url_2,
    tablet_image_url_3,
    category AS app_category,
    rn
   FROM rankedapps ra
  WHERE (rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_weekly OWNER TO postgres;

--
-- Name: idx_apps_fr_new_weekly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_fr_new_weekly ON frontend.apps_new_weekly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_weekly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_weekly ON frontend.apps_new_weekly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_weekly_f; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_weekly_f ON frontend.apps_new_weekly USING btree (store, app_category, store_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 1Ichnu339lmboR2IUTNEA27XnIqwwL0LhypdddfZIfDKpnQTJXYaLKZ4S5IYhJb


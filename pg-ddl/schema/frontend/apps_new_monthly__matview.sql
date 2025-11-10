--
-- PostgreSQL database dump
--

\restrict tbCdjCylwLafDiFcIUwYMlYs7aS2KF6sijqF3iOw5S5Z3FJDQJpNfXbCg2PcpIP

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
-- Name: apps_new_monthly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_monthly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
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
            sa_1.icon_url_512,
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
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '30 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '45 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT ra.id,
    ra.name,
    ra.store_id,
    ra.store,
    ra.category,
    ra.rating,
    ra.installs,
    ra.installs_sum_1w,
    ra.installs_sum_4w,
    ra.ratings_sum_1w,
    ra.ratings_sum_4w,
    ra.store_last_updated,
    ra.ad_supported,
    ra.in_app_purchases,
    ra.created_at,
    ra.updated_at,
    ra.crawl_result,
    sa.icon_url_100,
    ra.icon_url_512,
    ra.release_date,
    ra.rating_count,
    ra.featured_image_url,
    ra.phone_image_url_1,
    ra.phone_image_url_2,
    ra.phone_image_url_3,
    ra.tablet_image_url_1,
    ra.tablet_image_url_2,
    ra.tablet_image_url_3,
    ra.category AS app_category,
    ra.rn
   FROM (rankedapps ra
     LEFT JOIN public.store_apps sa ON ((ra.id = sa.id)))
  WHERE (ra.rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_monthly OWNER TO postgres;

--
-- Name: idx_apps_fr_new_monthly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_fr_new_monthly ON frontend.apps_new_monthly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_monthly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_monthly ON frontend.apps_new_monthly USING btree (store, app_category, store_id);


--
-- PostgreSQL database dump complete
--

\unrestrict tbCdjCylwLafDiFcIUwYMlYs7aS2KF6sijqF3iOw5S5Z3FJDQJpNfXbCg2PcpIP


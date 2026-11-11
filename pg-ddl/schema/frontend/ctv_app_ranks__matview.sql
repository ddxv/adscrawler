--
-- PostgreSQL database dump
--

\restrict wfix0G71mYKd4aPd3v3L4JSYf0SaN89zHT1iCyiRXhu3id3n4GLFnuuAWEF2EAT

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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
-- Name: ctv_app_ranks; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.ctv_app_ranks AS
 WITH ranked_apps AS (
         SELECT store_apps_overview.id,
            store_apps_overview.name,
            store_apps_overview.store_id,
            store_apps_overview.store,
            store_apps_overview.category,
            store_apps_overview.rating,
            store_apps_overview.rating_count,
            store_apps_overview.installs,
            store_apps_overview.installs_sum_1w,
            store_apps_overview.ratings_sum_1w,
            store_apps_overview.installs_sum_4w,
            store_apps_overview.installs_z_score_2w,
            store_apps_overview.installs_z_score_4w,
            store_apps_overview.weekly_active_users,
            store_apps_overview.monthly_active_users,
            store_apps_overview.weekly_ad_revenue,
            store_apps_overview.weekly_iap_revenue,
            store_apps_overview.monthly_ad_revenue,
            store_apps_overview.monthly_iap_revenue,
            store_apps_overview.ad_supported,
            store_apps_overview.free,
            store_apps_overview.in_app_purchases,
            store_apps_overview.store_last_updated,
            store_apps_overview.created_at,
            store_apps_overview.last_crawled_at,
            store_apps_overview.crawl_result,
            store_apps_overview.icon_64,
            store_apps_overview.icon_128,
            store_apps_overview.icon_url_512,
            store_apps_overview.release_date,
            store_apps_overview.tv_supported,
            store_apps_overview.featured_image_url,
            store_apps_overview.phone_image_url_1,
            store_apps_overview.phone_image_url_2,
            store_apps_overview.phone_image_url_3,
            store_apps_overview.tablet_image_url_1,
            store_apps_overview.tablet_image_url_2,
            store_apps_overview.tablet_image_url_3,
            store_apps_overview.textsearchable,
            store_apps_overview.developer_id,
            store_apps_overview.developer_name,
            store_apps_overview.developer_url,
            store_apps_overview.adstxt_last_crawled,
            store_apps_overview.adstxt_crawl_result,
            store_apps_overview.sdk_last_crawled,
            store_apps_overview.sdk_last_crawl_result,
            store_apps_overview.sdk_successful_last_crawled,
            store_apps_overview.description,
            store_apps_overview.description_short,
            store_apps_overview.api_last_crawled,
            store_apps_overview.run_result,
            store_apps_overview.api_successful_last_crawled,
            store_apps_overview.ad_creative_count,
            store_apps_overview.ad_mon_creatives,
            store_apps_overview.is_removed,
            store_apps_overview.country_id,
            row_number() OVER (PARTITION BY store_apps_overview.store ORDER BY store_apps_overview.installs_sum_4w DESC NULLS LAST) AS rn
           FROM frontend.store_apps_overview
          WHERE store_apps_overview.tv_supported
        )
 SELECT name AS app_name,
    store_id,
    store,
    COALESCE(icon_64, icon_128) AS app_icon,
    category AS app_category,
    developer_name,
    developer_id,
    rating,
    rating_count,
    installs,
    installs_sum_4w AS monthly_installs,
    (monthly_ad_revenue + monthly_iap_revenue) AS monthly_revenue,
    installs_z_score_2w,
    country_id,
    rn AS rank_num
   FROM ranked_apps
  WHERE (rn <= 50)
  ORDER BY store, installs_sum_4w DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.ctv_app_ranks OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict wfix0G71mYKd4aPd3v3L4JSYf0SaN89zHT1iCyiRXhu3id3n4GLFnuuAWEF2EAT


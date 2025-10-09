--
-- PostgreSQL database dump
--

\restrict tZA4PHWAt2sIbnzCFw1rfifg0EePa4KJ2mh09J3SGWUADKqZ417djnlpy7f3hgY

-- Dumped from database version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)
-- Dumped by pg_dump version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)

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
-- Name: store_apps_z_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_z_scores AS
WITH ranked_z_scores AS (
    SELECT
        saz.store_app,
        saz.latest_week,
        saz.installs_sum_1w,
        saz.ratings_sum_1w,
        saz.installs_avg_2w,
        saz.ratings_avg_2w,
        saz.installs_z_score_2w,
        saz.ratings_z_score_2w,
        saz.installs_sum_4w,
        saz.ratings_sum_4w,
        saz.installs_avg_4w,
        saz.ratings_avg_4w,
        saz.installs_z_score_4w,
        saz.ratings_z_score_4w,
        sa.id,
        sa.developer,
        sa.name,
        sa.store_id,
        sa.store,
        sa.category,
        sa.rating,
        sa.review_count,
        sa.installs,
        sa.free,
        sa.price,
        sa.size,
        sa.minimum_android,
        sa.developer_email,
        sa.store_last_updated,
        sa.content_rating,
        sa.ad_supported,
        sa.in_app_purchases,
        sa.editors_choice,
        sa.created_at,
        sa.updated_at,
        sa.crawl_result,
        sa.icon_url_512,
        sa.release_date,
        sa.rating_count,
        sa.featured_image_url,
        sa.phone_image_url_1,
        sa.phone_image_url_2,
        sa.phone_image_url_3,
        sa.tablet_image_url_1,
        sa.tablet_image_url_2,
        sa.tablet_image_url_3,
        sa.textsearchable_index_col,
        cm.original_category,
        cm.mapped_category,
        row_number() OVER (
            PARTITION BY
                sa.store, cm.mapped_category,
                CASE
                    WHEN (sa.store = 2) THEN 'rating'::text
                    ELSE 'installs'::text
                END ORDER BY
                CASE
                    WHEN (sa.store = 2) THEN saz.ratings_z_score_2w
                    WHEN (sa.store = 1) THEN saz.installs_z_score_2w
                    ELSE null::numeric
                END DESC NULLS LAST
        ) AS rn
    FROM ((
        public.store_app_z_scores saz
        LEFT JOIN public.store_apps AS sa ON ((saz.store_app = sa.id))
    )
    LEFT JOIN
        public.category_mapping AS cm
        ON (((sa.category)::text = (cm.original_category)::text))
    )
)
SELECT
    store,
    store_id,
    name AS app_name,
    mapped_category AS app_category,
    in_app_purchases,
    ad_supported,
    icon_url_512,
    installs,
    rating_count,
    installs_sum_1w,
    ratings_sum_1w,
    installs_avg_2w,
    ratings_avg_2w,
    installs_z_score_2w,
    ratings_z_score_2w,
    installs_sum_4w,
    ratings_sum_4w,
    installs_avg_4w,
    ratings_avg_4w,
    installs_z_score_4w,
    ratings_z_score_4w
FROM ranked_z_scores
WHERE (rn <= 100)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_z_scores OWNER TO postgres;

--
-- Name: frontend_store_apps_z_scores_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX frontend_store_apps_z_scores_unique ON frontend.store_apps_z_scores USING btree (
    store, store_id
);


--
-- PostgreSQL database dump complete
--

\unrestrict tZA4PHWAt2sIbnzCFw1rfifg0EePa4KJ2mh09J3SGWUADKqZ417djnlpy7f3hgY

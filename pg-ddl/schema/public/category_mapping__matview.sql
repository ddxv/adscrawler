--
-- PostgreSQL database dump
--

\restrict xK0PvE89JbZtm0UWrLpaToquGFHoMDgKXT9qewWo5lf1RbDduKUfFz6cGMAFqP2

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
-- Name: category_mapping; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.category_mapping AS
 SELECT DISTINCT original_category,
        CASE
            WHEN (mapped_category = ANY (ARRAY['action'::text, 'casual'::text, 'adventure'::text, 'arcade'::text, 'board'::text, 'card'::text, 'casino'::text, 'puzzle'::text, 'racing'::text, 'simulation'::text, 'strategy'::text, 'trivia'::text, 'word'::text])) THEN ('game_'::text || mapped_category)
            WHEN (mapped_category = 'news_and_magazines'::text) THEN 'news'::text
            WHEN (mapped_category = 'educational'::text) THEN 'education'::text
            WHEN (mapped_category = 'book'::text) THEN 'books_and_reference'::text
            WHEN (mapped_category = 'navigation'::text) THEN 'maps_and_navigation'::text
            WHEN (mapped_category = 'music'::text) THEN 'music_and_audio'::text
            WHEN (mapped_category = 'photography'::text) THEN 'photo_and_video'::text
            WHEN (mapped_category = 'reference'::text) THEN 'books_and_reference'::text
            WHEN (mapped_category = 'role playing'::text) THEN 'game_role_playing'::text
            WHEN (mapped_category = 'social'::text) THEN 'social networking'::text
            WHEN (mapped_category = 'travel'::text) THEN 'travel_and_local'::text
            WHEN (mapped_category = 'utilities'::text) THEN 'tools'::text
            WHEN (mapped_category = 'video players_and_editors'::text) THEN 'video_players'::text
            WHEN (mapped_category = 'graphics_and_design'::text) THEN 'art_and_design'::text
            WHEN (mapped_category = 'parenting'::text) THEN 'family'::text
            WHEN (mapped_category IS NULL) THEN 'N/A'::text
            ELSE mapped_category
        END AS mapped_category
   FROM ( SELECT DISTINCT store_apps.category AS original_category,
            regexp_replace(lower((store_apps.category)::text), ' & '::text, '_and_'::text) AS mapped_category
           FROM public.store_apps) sub
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.category_mapping OWNER TO postgres;

--
-- Name: category_mapping_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX category_mapping_idx ON public.category_mapping USING btree (original_category, mapped_category);


--
-- PostgreSQL database dump complete
--

\unrestrict xK0PvE89JbZtm0UWrLpaToquGFHoMDgKXT9qewWo5lf1RbDduKUfFz6cGMAFqP2


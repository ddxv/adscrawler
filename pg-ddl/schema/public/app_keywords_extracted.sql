--
-- PostgreSQL database dump
--

\restrict SJS4RcJjnqRUvTtIYVkyuoz2MckHTRieXUtWYUfNOJVxUYPAjxSTtqdHfHAJIHN

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
-- Name: app_keywords_extracted; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_keywords_extracted (
    store_app integer NOT NULL,
    keyword_id integer NOT NULL,
    description_id integer NOT NULL,
    extracted_at timestamp without time zone NOT NULL
);


ALTER TABLE public.app_keywords_extracted OWNER TO postgres;

--
-- Name: app_keywords_extracted description_keywords_app_id_keyword_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keywords_extracted
    ADD CONSTRAINT description_keywords_app_id_keyword_id_key UNIQUE (store_app, keyword_id);


--
-- Name: ake_latest_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ake_latest_idx ON public.app_keywords_extracted USING btree (store_app, extracted_at DESC);


--
-- Name: app_keywords_app_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_keywords_app_index ON public.app_keywords_extracted USING btree (store_app);


--
-- Name: app_keywords_extracted description_keywords_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keywords_extracted
    ADD CONSTRAINT description_keywords_app_id_fkey FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: app_keywords_extracted description_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keywords_extracted
    ADD CONSTRAINT description_keywords_keyword_id_fkey FOREIGN KEY (keyword_id) REFERENCES public.keywords(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict SJS4RcJjnqRUvTtIYVkyuoz2MckHTRieXUtWYUfNOJVxUYPAjxSTtqdHfHAJIHN


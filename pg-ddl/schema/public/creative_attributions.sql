--
-- PostgreSQL database dump
--

\restrict 70KQ1xf0vJjpHhrKQaVAL0wtUEVEwFfOcr5XVyORjs1saGg1rdh7hTwzksLkXlW

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
-- Name: creative_attributions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_attributions (
    id integer NOT NULL,
    api_call_id integer NOT NULL,
    creative_asset_id integer NOT NULL,
    creative_initial_domain_id integer NOT NULL,
    creative_host_domain_id integer NOT NULL,
    mmp_domain_id integer,
    mmp_urls text [],
    additional_ad_domain_ids integer [],
    advertiser_store_app_id integer,
    click_domain_id integer,
    created_at timestamp with time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL,
    updated_at timestamp with time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL,
    CONSTRAINT check_advertiser_or_ecommerce CHECK (
        (
            (advertiser_store_app_id IS NOT null)
            OR (click_domain_id IS NOT null)
            OR ((advertiser_store_app_id IS null) AND (click_domain_id IS null))
        )
    )
);


ALTER TABLE public.creative_attributions OWNER TO postgres;

--
-- Name: creative_attributions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_attributions_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.creative_attributions_id_seq OWNER TO postgres;

--
-- Name: creative_attributions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_attributions_id_seq OWNED BY public.creative_attributions.id;


--
-- Name: creative_attributions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions ALTER COLUMN id SET DEFAULT nextval(
    'public.creative_attributions_id_seq'::regclass
);


--
-- Name: creative_attributions creative_attributions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_pkey PRIMARY KEY (id);


--
-- Name: creative_attributions creative_attributions_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_unique UNIQUE (
    api_call_id,
    creative_asset_id,
    creative_initial_domain_id,
    creative_host_domain_id
);


--
-- Name: creative_attributions creative_attributions_advertiser_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_advertiser_fk FOREIGN KEY (
    advertiser_store_app_id
) REFERENCES public.store_apps (id);


--
-- Name: creative_attributions creative_attributions_asset_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_asset_fk FOREIGN KEY (
    creative_asset_id
) REFERENCES public.creative_assets (id);


--
-- Name: creative_attributions creative_attributions_ecommerce_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_ecommerce_domain_fk FOREIGN KEY (
    click_domain_id
) REFERENCES public.ad_domains (id);


--
-- Name: creative_attributions creative_attributions_host_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_host_domain_fk FOREIGN KEY (
    creative_host_domain_id
) REFERENCES public.ad_domains (id);


--
-- Name: creative_attributions creative_attributions_initial_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_initial_domain_fk FOREIGN KEY (
    creative_initial_domain_id
) REFERENCES public.ad_domains (id);


--
-- Name: creative_attributions creative_attributions_mmp_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_attributions
ADD CONSTRAINT creative_attributions_mmp_domain_fk FOREIGN KEY (
    mmp_domain_id
) REFERENCES public.ad_domains (id);


--
-- PostgreSQL database dump complete
--

\unrestrict 70KQ1xf0vJjpHhrKQaVAL0wtUEVEwFfOcr5XVyORjs1saGg1rdh7hTwzksLkXlW

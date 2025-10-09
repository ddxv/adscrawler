--
-- PostgreSQL database dump
--

\restrict SWcKBehQx01Offi7MMZnrfsQvLFflNle1gMuJhbHL5OghvxlLj3TXjsPA2XDY8t

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
-- Name: companies_open_source_percent; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_open_source_percent AS
SELECT
    ad.domain AS company_domain,
    avg(
        CASE
            WHEN sd.is_open_source THEN 1
            ELSE 0
        END
    ) AS percent_open_source
FROM ((
    adtech.sdks sd
    LEFT JOIN adtech.companies AS c ON ((sd.company_id = c.id))
)
LEFT JOIN public.ad_domains AS ad ON ((c.domain_id = ad.id))
)
GROUP BY ad.domain
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_open_source_percent OWNER TO postgres;

--
-- Name: companies_open_source_percent_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_open_source_percent_unique ON frontend.companies_open_source_percent USING btree (
    company_domain
);


--
-- PostgreSQL database dump complete
--

\unrestrict SWcKBehQx01Offi7MMZnrfsQvLFflNle1gMuJhbHL5OghvxlLj3TXjsPA2XDY8t

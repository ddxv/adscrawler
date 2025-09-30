--
-- PostgreSQL database dump
--

\restrict nWTyAjTVbTPxnwRUzIFgLVHEdaMzNnMoTc6U6tWSP6oQZp9a59C3svkbMwCfcRB

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
-- Name: company_domains_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domains_top_apps AS
WITH deduped_data AS (
    SELECT DISTINCT
        saac.tld_url AS company_domain,
        c_1.name AS company_name,
        sa.store,
        sa.name,
        sa.store_id,
        cm.mapped_category AS app_category,
        sa.installs_sum_4w AS installs_d30,
        sa.ratings_sum_4w AS rating_count_d30,
        false AS sdk,
        true AS api_call,
        false AS app_ads_direct
    FROM ((((((
        public.store_app_api_calls saac
        LEFT JOIN public.store_apps AS sa_1 ON ((saac.store_app = sa_1.id))
    )
    LEFT JOIN
        public.category_mapping AS cm
        ON (((sa_1.category)::text = (cm.original_category)::text))
    )
    LEFT JOIN
        public.ad_domains AS ad_1
        ON ((saac.tld_url = (ad_1.domain)::text))
    )
    LEFT JOIN
        adtech.company_domain_mapping AS cdm
        ON ((ad_1.id = cdm.domain_id))
    )
    LEFT JOIN adtech.companies AS c_1 ON ((cdm.company_id = c_1.id))
    )
    LEFT JOIN frontend.store_apps_overview AS sa ON ((saac.store_app = sa.id))
    )
), ranked_apps AS (
    SELECT
        deduped_data.company_domain,
        deduped_data.company_name,
        deduped_data.store,
        deduped_data.name,
        deduped_data.store_id,
        deduped_data.app_category,
        deduped_data.installs_d30,
        deduped_data.rating_count_d30,
        deduped_data.sdk,
        deduped_data.api_call,
        deduped_data.app_ads_direct,
        row_number()
            OVER (
                PARTITION BY
                    deduped_data.store,
                    deduped_data.company_domain,
                    deduped_data.company_name
                ORDER BY
                    greatest(
                        (
                            coalesce(
                                deduped_data.rating_count_d30, (0)::numeric
                            )
                        )::double precision,
                        coalesce(
                            (deduped_data.installs_d30)::double precision,
                            (0)::double precision
                        )
                    ) DESC
            )
            AS app_company_rank,
        row_number()
            OVER (
                PARTITION BY
                    deduped_data.store,
                    deduped_data.app_category,
                    deduped_data.company_domain,
                    deduped_data.company_name
                ORDER BY
                    greatest(
                        (
                            coalesce(
                                deduped_data.rating_count_d30, (0)::numeric
                            )
                        )::double precision,
                        coalesce(
                            (deduped_data.installs_d30)::double precision,
                            (0)::double precision
                        )
                    ) DESC
            )
            AS app_company_category_rank
    FROM deduped_data
)
SELECT
    company_domain,
    company_name,
    store,
    name,
    store_id,
    app_category,
    installs_d30,
    rating_count_d30,
    sdk,
    api_call,
    app_ads_direct,
    app_company_rank,
    app_company_category_rank
FROM ranked_apps
WHERE (app_company_category_rank <= 100)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_domains_top_apps OWNER TO postgres;

--
-- Name: idx_company_top_domains_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_domains_apps ON frontend.company_domains_top_apps USING btree (
    company_domain
);


--
-- Name: idx_company_top_domains_apps_domain_rank; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_domains_apps_domain_rank ON frontend.company_domains_top_apps USING btree (
    company_domain, app_company_rank
);


--
-- Name: idx_query_company_domains_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_query_company_domains_top_apps ON frontend.company_domains_top_apps USING btree (
    company_domain, app_category, app_company_category_rank, store
);


--
-- Name: idx_unique_company_domains_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_unique_company_domains_top_apps ON frontend.company_domains_top_apps USING btree (
    company_domain, company_name, store, name, store_id, app_category
);


--
-- PostgreSQL database dump complete
--

\unrestrict nWTyAjTVbTPxnwRUzIFgLVHEdaMzNnMoTc6U6tWSP6oQZp9a59C3svkbMwCfcRB

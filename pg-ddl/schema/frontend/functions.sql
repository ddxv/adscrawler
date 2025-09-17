--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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

--
-- Name: frontend; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA frontend;


ALTER SCHEMA frontend OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: store_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_overview AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at AS last_downloaded_at,
            version_codes.crawl_result AS download_result
           FROM public.version_codes
          WHERE ((version_codes.crawl_result = 1) AND (version_codes.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone))
          ORDER BY version_codes.store_app, version_codes.updated_at DESC, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), latest_successful_version_codes AS (
         SELECT DISTINCT ON (vc.store_app) vc.id,
            vc.store_app,
            vc.version_code,
            vc.updated_at,
            vc.crawl_result
           FROM public.version_codes vc
          WHERE (vc.crawl_result = 1)
          ORDER BY vc.store_app, (string_to_array((vc.version_code)::text, '.'::text))::bigint[] DESC
        ), last_sdk_scan AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            lsscr.version_code_id AS version_code,
            lsscr.scanned_at,
            lsscr.scan_result
           FROM (public.version_code_sdk_scan_results lsscr
             LEFT JOIN public.version_codes vc ON ((lsscr.version_code_id = vc.id)))
          ORDER BY vc.store_app, lsscr.scanned_at DESC
        ), last_successful_sdk_scan AS (
         SELECT DISTINCT ON (vc.store_app) vc.id,
            vc.store_app,
            vc.version_code,
            vcss.scanned_at,
            vcss.scan_result
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_sdk_scan_results vcss ON ((vc.id = vcss.version_code_id)))
          WHERE (vcss.scan_result = 1)
          ORDER BY vc.store_app, vcss.scanned_at DESC, (string_to_array((vc.version_code)::text, '.'::text))::bigint[] DESC
        ), latest_en_descriptions AS (
         SELECT DISTINCT ON (store_apps_descriptions.store_app) store_apps_descriptions.store_app,
            store_apps_descriptions.description,
            store_apps_descriptions.description_short
           FROM public.store_apps_descriptions
          WHERE (store_apps_descriptions.language_id = 1)
          ORDER BY store_apps_descriptions.store_app, store_apps_descriptions.updated_at DESC
        ), latest_api_calls AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            vasr.run_result,
            vasr.run_at
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_api_scan_results vasr ON ((vasr.version_code_id = vc.id)))
          WHERE (vc.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone)
        ), latest_successful_api_calls AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            vasr.run_at
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_api_scan_results vasr ON ((vasr.version_code_id = vc.id)))
          WHERE ((vasr.run_result = 1) AND (vc.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone))
        ), my_ad_creatives AS (
         SELECT ca.store_app_id,
            count(*) AS ad_creative_count
           FROM (public.creative_records cr
             LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
          GROUP BY ca.store_app_id
        )
 SELECT sa.id,
    sa.name,
    sa.store_id,
    sa.store,
    cm.mapped_category AS category,
    sa.rating,
    sa.rating_count,
    sa.review_count,
    sa.installs,
    saz.installs_sum_1w,
    saz.ratings_sum_1w,
    saz.installs_sum_4w,
    saz.ratings_sum_4w,
    saz.installs_z_score_2w,
    saz.ratings_z_score_2w,
    saz.installs_z_score_4w,
    saz.ratings_z_score_4w,
    sa.ad_supported,
    sa.in_app_purchases,
    sa.store_last_updated,
    sa.created_at,
    sa.updated_at,
    sa.crawl_result,
    sa.icon_url_512,
    sa.release_date,
    sa.featured_image_url,
    sa.phone_image_url_1,
    sa.phone_image_url_2,
    sa.phone_image_url_3,
    sa.tablet_image_url_1,
    sa.tablet_image_url_2,
    sa.tablet_image_url_3,
    d.developer_id,
    d.name AS developer_name,
    pd.url AS developer_url,
    pd.updated_at AS adstxt_last_crawled,
    pd.crawl_result AS adstxt_crawl_result,
    lss.scanned_at AS sdk_last_crawled,
    lss.scan_result AS sdk_last_crawl_result,
    lsss.scanned_at AS sdk_successful_last_crawled,
    lvc.version_code,
    ld.description,
    ld.description_short,
    lac.run_at AS api_last_crawled,
    lac.run_result,
    lsac.run_at AS api_successful_last_crawled,
    acr.ad_creative_count
   FROM (((((((((((((public.store_apps sa
     LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
     LEFT JOIN public.developers d ON ((sa.developer = d.id)))
     LEFT JOIN public.app_urls_map aum ON ((sa.id = aum.store_app)))
     LEFT JOIN public.pub_domains pd ON ((aum.pub_domain = pd.id)))
     LEFT JOIN latest_version_codes lvc ON ((sa.id = lvc.store_app)))
     LEFT JOIN last_sdk_scan lss ON ((sa.id = lss.store_app)))
     LEFT JOIN last_successful_sdk_scan lsss ON ((sa.id = lsss.store_app)))
     LEFT JOIN latest_successful_version_codes lsvc ON ((sa.id = lsvc.store_app)))
     LEFT JOIN latest_en_descriptions ld ON ((sa.id = ld.store_app)))
     LEFT JOIN public.store_app_z_scores saz ON ((sa.id = saz.store_app)))
     LEFT JOIN latest_api_calls lac ON ((sa.id = lac.store_app)))
     LEFT JOIN latest_successful_api_calls lsac ON ((sa.id = lsac.store_app)))
     LEFT JOIN my_ad_creatives acr ON ((sa.id = acr.store_app_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_overview OWNER TO postgres;

--
-- Name: adstxt_entries_store_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_entries_store_apps AS
 WITH parent_companies AS (
         SELECT c.id AS company_id,
            c.name AS company_name,
            COALESCE(c.parent_company_id, c.id) AS parent_company_id,
            COALESCE(pc.name, c.name) AS parent_company_name,
            COALESCE(pc.domain_id, c.domain_id) AS parent_company_domain_id
           FROM (adtech.companies c
             LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
        )
 SELECT DISTINCT ad.id AS ad_domain_id,
    myc.parent_company_id AS company_id,
    aae.id AS app_ad_entry_id,
    sa.id AS store_app,
    pd.id AS pub_domain_id
   FROM ((((((public.app_ads_entrys aae
     LEFT JOIN public.ad_domains ad ON ((aae.ad_domain = ad.id)))
     LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
     LEFT JOIN public.pub_domains pd ON ((aam.pub_domain = pd.id)))
     LEFT JOIN public.app_urls_map aum ON ((pd.id = aum.pub_domain)))
     JOIN public.store_apps sa ON ((aum.store_app = sa.id)))
     LEFT JOIN parent_companies myc ON ((ad.id = myc.parent_company_domain_id)))
  WHERE ((pd.crawled_at - aam.updated_at) < '01:00:00'::interval)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_entries_store_apps OWNER TO postgres;

--
-- Name: adstxt_ad_domain_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_ad_domain_overview AS
 SELECT ad.domain AS ad_domain_url,
    aae.relationship,
    sa.store,
    count(DISTINCT aae.publisher_id) AS publisher_id_count,
    count(DISTINCT sa.developer) AS developer_count,
    count(DISTINCT aesa.store_app) AS app_count
   FROM (((frontend.adstxt_entries_store_apps aesa
     LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
     LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
     LEFT JOIN public.ad_domains ad ON ((aesa.ad_domain_id = ad.id)))
  GROUP BY ad.domain, aae.relationship, sa.store
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_ad_domain_overview OWNER TO postgres;

--
-- Name: adstxt_publishers_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_publishers_overview AS
 WITH ranked_data AS (
         SELECT ad.domain AS ad_domain_url,
            aae.relationship,
            sa.store,
            aae.publisher_id,
            count(DISTINCT sa.developer) AS developer_count,
            count(DISTINCT aesa.store_app) AS app_count,
            row_number() OVER (PARTITION BY ad.domain, aae.relationship, sa.store ORDER BY (count(DISTINCT aesa.store_app)) DESC) AS pubrank
           FROM (((frontend.adstxt_entries_store_apps aesa
             LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
             LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
             LEFT JOIN public.ad_domains ad ON ((aesa.ad_domain_id = ad.id)))
          GROUP BY ad.domain, aae.relationship, sa.store, aae.publisher_id
        )
 SELECT ad_domain_url,
    relationship,
    store,
    publisher_id,
    developer_count,
    app_count,
    pubrank
   FROM ranked_data rd
  WHERE (pubrank <= 50)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_publishers_overview OWNER TO postgres;

--
-- Name: advertiser_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings AS
 WITH adv_mmp AS (
         SELECT DISTINCT ca_1.store_app_id AS advertiser_store_app_id,
            cr_1.mmp_domain_id,
            ad.domain AS mmp_domain
           FROM ((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.ad_domains ad ON ((cr_1.mmp_domain_id = ad.id)))
          WHERE (cr_1.mmp_domain_id IS NOT NULL)
        ), creative_rankings AS (
         SELECT ca_1.md5_hash,
            ca_1.file_extension,
            ca_1.store_app_id AS advertiser_store_app_id,
            vcasr_1.run_at,
            row_number() OVER (PARTITION BY ca_1.store_app_id ORDER BY vcasr_1.run_at DESC) AS rn
           FROM ((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((cr_1.run_id = vcasr_1.id)))
        )
 SELECT saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_100 AS advertiser_icon_url_100,
    saa.icon_url_512 AS advertiser_icon_url_512,
    saa.category AS advertiser_category,
    saa.installs AS advertiser_installs,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT cr.store_app_pub_id) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    avg(sap.installs) AS avg_publisher_installs,
    array_agg(DISTINCT adv_mmp.mmp_domain) AS mmp_domains,
    ARRAY( SELECT crk.md5_hash
           FROM creative_rankings crk
          WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
          ORDER BY crk.rn) AS top_md5_hashes
   FROM (((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((cr.store_app_pub_id = sap.id)))
     LEFT JOIN public.store_apps saa ON ((ca.store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((cr.run_id = vcasr.id)))
     LEFT JOIN adv_mmp ON ((ca.store_app_id = adv_mmp.advertiser_store_app_id)))
  GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.category, saa.installs, saa.id
  ORDER BY (count(DISTINCT ca.md5_hash)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings OWNER TO postgres;

--
-- Name: advertiser_creative_rankings_recent_month; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings_recent_month AS
 WITH creative_rankings AS (
         SELECT ca_1.md5_hash,
            ca_1.file_extension,
            ca_1.store_app_id AS advertiser_store_app_id,
            vcasr_1.run_at,
            row_number() OVER (PARTITION BY ca_1.store_app_id ORDER BY vcasr_1.run_at DESC) AS rn
           FROM ((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((cr_1.run_id = vcasr_1.id)))
        )
 SELECT saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_100 AS advertiser_icon_url_100,
    saa.icon_url_512 AS advertiser_icon_url_512,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT cr.store_app_pub_id) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    avg(sap.installs) AS avg_publisher_installs,
    ARRAY( SELECT crk.md5_hash
           FROM creative_rankings crk
          WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
          ORDER BY crk.rn) AS top_md5_hashes
   FROM ((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((cr.store_app_pub_id = sap.id)))
     LEFT JOIN public.store_apps saa ON ((ca.store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((cr.run_id = vcasr.id)))
  WHERE (vcasr.run_at >= (now() - '1 mon'::interval))
  GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.id
  ORDER BY (count(DISTINCT cr.store_app_pub_id)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings_recent_month OWNER TO postgres;

--
-- Name: advertiser_creatives; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creatives AS
 SELECT saa.store_id AS advertiser_store_id,
    cr.run_id,
    vcasr.run_at,
    sap.name AS pub_name,
    saa.name AS adv_name,
    sap.store_id AS pub_store_id,
    saa.store_id AS adv_store_id,
    hd.domain AS host_domain,
    COALESCE(hcd.domain, hd.domain) AS host_domain_company_domain,
    hc.name AS host_domain_company_name,
    ad.domain AS ad_domain,
    COALESCE(acd.domain, ad.domain) AS ad_domain_company_domain,
    ac.name AS ad_domain_company_name,
    COALESCE(ca.phash, ca.md5_hash) AS vhash,
    ca.md5_hash,
    ca.file_extension,
    sap.icon_url_512 AS pub_icon_url_512,
    saa.icon_url_512 AS adv_icon_url_512,
    mmp.name AS mmp_name,
    mmpd.domain AS mmp_domain,
    cr.mmp_urls,
    ( SELECT array_agg(ad_domains.domain) AS array_agg
           FROM public.ad_domains
          WHERE (ad_domains.id = ANY (cr.additional_ad_domain_ids))) AS additional_ad_domain_urls
   FROM (((((((((((((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((cr.store_app_pub_id = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((ca.store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((cr.run_id = vcasr.id)))
     LEFT JOIN public.ad_domains hd ON ((cr.creative_host_domain_id = hd.id)))
     LEFT JOIN public.ad_domains ad ON ((cr.creative_initial_domain_id = ad.id)))
     LEFT JOIN adtech.company_domain_mapping hcdm ON ((hd.id = hcdm.domain_id)))
     LEFT JOIN adtech.company_domain_mapping acdm ON ((ad.id = acdm.domain_id)))
     LEFT JOIN adtech.companies hc ON ((hcdm.company_id = hc.id)))
     LEFT JOIN adtech.companies ac ON ((acdm.company_id = ac.id)))
     LEFT JOIN public.ad_domains hcd ON ((hc.domain_id = hcd.id)))
     LEFT JOIN public.ad_domains acd ON ((ac.domain_id = acd.id)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((cr.mmp_domain_id = cdm.domain_id)))
     LEFT JOIN adtech.companies mmp ON ((cdm.company_id = mmp.id)))
     LEFT JOIN public.ad_domains mmpd ON ((cr.mmp_domain_id = mmpd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creatives OWNER TO postgres;

--
-- Name: api_call_countries; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.api_call_countries AS
 WITH latest_run_per_app AS (
         SELECT DISTINCT ON (saac.store_app) saac.store_app,
            saac.run_id
           FROM (public.store_app_api_calls saac
             JOIN public.version_code_api_scan_results vcasr ON ((saac.run_id = vcasr.id)))
          WHERE (saac.country_id IS NOT NULL)
          ORDER BY saac.store_app, vcasr.run_at DESC
        ), filtered_calls AS (
         SELECT saac.store_app,
            saac.tld_url,
            saac.url,
            saac.country_id,
            saac.city_name,
            saac.org
           FROM (public.store_app_api_calls saac
             JOIN latest_run_per_app lra ON (((saac.store_app = lra.store_app) AND (saac.run_id = lra.run_id))))
        ), cleaned_calls AS (
         SELECT filtered_calls.store_app,
            filtered_calls.tld_url,
            filtered_calls.country_id,
            filtered_calls.city_name,
            filtered_calls.org,
            regexp_replace(regexp_replace(regexp_replace(filtered_calls.url, '^https?://'::text, ''::text), '\?.*$'::text, ''::text), '^(([^/]+/){0,2}[^/]+).*$'::text, '\1'::text) AS short_url
           FROM filtered_calls
        )
 SELECT COALESCE(cad.domain, (ca.tld_url)::character varying) AS company_domain,
    COALESCE(pcad.domain, COALESCE(cad.domain, (ca.tld_url)::character varying)) AS parent_company_domain,
    ca.tld_url,
    co.alpha2 AS country,
    ca.org,
    count(DISTINCT ca.store_app) AS store_app_count
   FROM (((((((cleaned_calls ca
     LEFT JOIN public.ad_domains ad ON ((ca.tld_url = (ad.domain)::text)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ad.id = cdm.domain_id)))
     LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
     LEFT JOIN public.ad_domains cad ON ((c.domain_id = cad.id)))
     LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
     LEFT JOIN public.ad_domains pcad ON ((pc.domain_id = pcad.id)))
     LEFT JOIN public.countries co ON ((ca.country_id = co.id)))
  GROUP BY COALESCE(cad.domain, (ca.tld_url)::character varying), COALESCE(pcad.domain, COALESCE(cad.domain, (ca.tld_url)::character varying)), ca.tld_url, co.alpha2, ca.org
  ORDER BY (count(DISTINCT ca.store_app)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.api_call_countries OWNER TO postgres;

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
            sa_1.review_count,
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
    ra.review_count,
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
     LEFT JOIN public.store_apps sa ON ((sa.id = ra.id)))
  WHERE (ra.rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_monthly OWNER TO postgres;

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
            sa_1.rating,
            sa_1.review_count,
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
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '7 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '11 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT ra.id,
    ra.name,
    ra.store_id,
    ra.store,
    ra.category,
    ra.rating,
    ra.review_count,
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
     LEFT JOIN public.store_apps sa ON ((sa.id = ra.id)))
  WHERE (ra.rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_weekly OWNER TO postgres;

--
-- Name: apps_new_yearly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_yearly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
            sa_1.rating,
            sa_1.review_count,
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
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '365 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '380 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT ra.id,
    ra.name,
    ra.store_id,
    ra.store,
    ra.category,
    ra.rating,
    ra.review_count,
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
     LEFT JOIN public.store_apps sa ON ((sa.id = ra.id)))
  WHERE (ra.rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_yearly OWNER TO postgres;

--
-- Name: category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.category_tag_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            sa.installs,
            sa.rating_count
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.category_tag_stats OWNER TO postgres;

--
-- Name: store_apps_version_details; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_version_details AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at,
            version_codes.crawl_result
           FROM public.version_codes
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        )
 SELECT DISTINCT vs.id AS version_string_id,
    sa.store,
    sa.store_id,
    cvsm.company_id,
    c.name AS company_name,
    ad.domain AS company_domain,
    cats.url_slug AS category_slug
   FROM ((((((((latest_version_codes vc
     LEFT JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
     LEFT JOIN public.version_strings vs ON ((vdm.string_id = vs.id)))
     LEFT JOIN adtech.company_value_string_mapping cvsm ON ((vs.id = cvsm.version_string_id)))
     LEFT JOIN adtech.companies c ON ((cvsm.company_id = c.id)))
     LEFT JOIN adtech.company_categories cc ON ((c.id = cc.company_id)))
     LEFT JOIN adtech.categories cats ON ((cc.category_id = cats.id)))
     LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN public.store_apps sa ON ((vc.store_app = sa.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_version_details OWNER TO postgres;

--
-- Name: companies_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_apps_overview AS
 SELECT DISTINCT store_id,
    company_id,
    company_name,
    company_domain,
    category_slug
   FROM frontend.store_apps_version_details
  WHERE (company_id IS NOT NULL)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_apps_overview OWNER TO postgres;

--
-- Name: companies_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
        )
 SELECT dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_stats OWNER TO postgres;

--
-- Name: companies_category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_tag_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM (((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_stats OWNER TO postgres;

--
-- Name: companies_category_tag_type_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_tag_type_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        )
 SELECT sa.store,
    csac.app_category,
    tag.tag_source,
    csac.ad_domain AS company_domain,
    c.name AS company_name,
        CASE
            WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
            ELSE cats.url_slug
        END AS type_url_slug,
    count(DISTINCT csac.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(sa.installs) AS installs_total,
    sum(sa.rating_count) AS rating_count_total
   FROM ((((((adtech.combined_store_apps_companies csac
     LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
     LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
     LEFT JOIN d30_counts dc ON ((csac.store_app = dc.store_app)))
     LEFT JOIN adtech.company_categories ccats ON ((csac.company_id = ccats.company_id)))
     LEFT JOIN adtech.categories cats ON ((ccats.category_id = cats.id)))
     CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
  WHERE (tag.present IS TRUE)
  GROUP BY sa.store, csac.app_category, tag.tag_source, csac.ad_domain, c.name,
        CASE
            WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
            ELSE cats.url_slug
        END
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_type_stats OWNER TO postgres;

--
-- Name: companies_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_creative_rankings AS
 WITH creative_rankings AS (
         SELECT COALESCE(ca.phash, ca.md5_hash) AS vhash,
            ca.file_extension,
            ca.store_app_id AS advertiser_store_app_id,
            cr.creative_initial_domain_id,
            cr.creative_host_domain_id,
            cr.additional_ad_domain_ids,
            vcasr.run_at,
            ca.md5_hash
           FROM ((public.creative_records cr
             LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr ON ((cr.run_id = vcasr.id)))
        ), combined_domains AS (
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_initial_domain_id AS domain_id,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_host_domain_id,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            unnest(cr.additional_ad_domain_ids) AS unnest,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        ), visually_distinct AS (
         SELECT cdm.company_id,
            cd.file_extension,
            cd.advertiser_store_app_id,
            cd.vhash,
            min((cd.md5_hash)::text) AS md5_hash,
            max(cd.run_at) AS last_seen
           FROM (combined_domains cd
             LEFT JOIN adtech.company_domain_mapping cdm ON ((cd.domain_id = cdm.domain_id)))
          GROUP BY cdm.company_id, cd.file_extension, cd.advertiser_store_app_id, cd.vhash
        )
 SELECT DISTINCT vd.company_id,
    vd.md5_hash,
    vd.file_extension,
    ad.domain AS company_domain,
    sa.store_id AS advertiser_store_id,
    sa.icon_url_512,
    vd.last_seen
   FROM (((visually_distinct vd
     LEFT JOIN adtech.companies c ON ((vd.company_id = c.id)))
     LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN frontend.store_apps_overview sa ON ((vd.advertiser_store_app_id = sa.id)))
  WHERE (c.id IS NOT NULL)
  ORDER BY vd.last_seen DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_creative_rankings OWNER TO postgres;

--
-- Name: companies_open_source_percent; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_open_source_percent AS
 SELECT ad.domain AS company_domain,
    avg(
        CASE
            WHEN sd.is_open_source THEN 1
            ELSE 0
        END) AS percent_open_source
   FROM ((adtech.sdks sd
     LEFT JOIN adtech.companies c ON ((sd.company_id = c.id)))
     LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
  GROUP BY ad.domain
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_open_source_percent OWNER TO postgres;

--
-- Name: companies_parent_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_parent_category_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            COALESCE(ad.domain, csac.ad_domain) AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM (((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
             LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
          WHERE (csac.parent_id IN ( SELECT DISTINCT pc.id
                   FROM (adtech.companies pc
                     LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
                  WHERE (c_1.id IS NOT NULL)))
        )
 SELECT dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_parent_category_stats OWNER TO postgres;

--
-- Name: companies_parent_category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_parent_category_tag_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            COALESCE(ad.domain, csac.ad_domain) AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM ((((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
             LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE ((tag.present IS TRUE) AND (csac.parent_id IN ( SELECT DISTINCT pc.id
                   FROM (adtech.companies pc
                     LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
                  WHERE (c_1.id IS NOT NULL))))
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_parent_category_tag_stats OWNER TO postgres;

--
-- Name: companies_sdks_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_sdks_overview AS
 SELECT c.name AS company_name,
    ad.domain AS company_domain,
    parad.domain AS parent_company_domain,
    sdk.sdk_name,
    sp.package_pattern,
    sp2.path_pattern,
    COALESCE(cc.name, c.name) AS parent_company_name
   FROM ((((((adtech.companies c
     LEFT JOIN adtech.companies cc ON ((c.parent_company_id = cc.id)))
     LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN public.ad_domains parad ON ((cc.domain_id = parad.id)))
     LEFT JOIN adtech.sdks sdk ON ((c.id = sdk.company_id)))
     LEFT JOIN adtech.sdk_packages sp ON ((sdk.id = sp.sdk_id)))
     LEFT JOIN adtech.sdk_paths sp2 ON ((sdk.id = sp2.sdk_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_sdks_overview OWNER TO postgres;

--
-- Name: companies_version_details_count; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_version_details_count AS
 SELECT savd.store,
    savd.company_name,
    savd.company_domain,
    vs.xml_path,
    vs.value_name,
    count(DISTINCT savd.store_id) AS app_count
   FROM (frontend.store_apps_version_details savd
     LEFT JOIN public.version_strings vs ON ((savd.version_string_id = vs.id)))
  GROUP BY savd.store, savd.company_name, savd.company_domain, vs.xml_path, vs.value_name
  ORDER BY (count(DISTINCT savd.store_id)) DESC
 LIMIT 1000
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_version_details_count OWNER TO postgres;

--
-- Name: company_domain_country; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domain_country AS
 WITH country_totals AS (
         SELECT api_call_countries.company_domain,
            api_call_countries.country,
            sum(api_call_countries.store_app_count) AS total_app_count
           FROM frontend.api_call_countries
          GROUP BY api_call_countries.company_domain, api_call_countries.country
        ), parent_country_totals AS (
         SELECT api_call_countries.parent_company_domain,
            api_call_countries.country,
            sum(api_call_countries.store_app_count) AS total_app_count
           FROM frontend.api_call_countries
          GROUP BY api_call_countries.parent_company_domain, api_call_countries.country
        ), parent_ranked_countries AS (
         SELECT parent_country_totals.parent_company_domain,
            parent_country_totals.country,
            parent_country_totals.total_app_count,
            row_number() OVER (PARTITION BY parent_country_totals.parent_company_domain ORDER BY parent_country_totals.total_app_count DESC) AS rn
           FROM parent_country_totals
        ), company_ranked_countries AS (
         SELECT country_totals.company_domain,
            country_totals.country,
            country_totals.total_app_count,
            row_number() OVER (PARTITION BY country_totals.company_domain ORDER BY country_totals.total_app_count DESC) AS rn
           FROM country_totals
        )
 SELECT company_ranked_countries.company_domain,
    company_ranked_countries.country AS most_common_country,
    company_ranked_countries.total_app_count
   FROM company_ranked_countries
  WHERE ((NOT ((company_ranked_countries.company_domain)::text IN ( SELECT parent_ranked_countries.parent_company_domain
           FROM parent_ranked_countries))) AND (company_ranked_countries.rn = 1))
UNION
 SELECT parent_ranked_countries.parent_company_domain AS company_domain,
    parent_ranked_countries.country AS most_common_country,
    parent_ranked_countries.total_app_count
   FROM parent_ranked_countries
  WHERE (parent_ranked_countries.rn = 1)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_domain_country OWNER TO postgres;

--
-- Name: company_domains_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domains_top_apps AS
 WITH deduped_data AS (
         SELECT DISTINCT saac.tld_url AS company_domain,
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
           FROM ((((((public.store_app_api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
             LEFT JOIN public.ad_domains ad_1 ON ((saac.tld_url = (ad_1.domain)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((ad_1.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((saac.store_app = sa.id)))
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
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
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
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
-- Name: company_parent_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_parent_top_apps AS
 WITH deduped_data AS (
         SELECT csapc.ad_domain AS company_domain,
            c.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            csapc.app_category,
            sa.installs_sum_4w AS installs_d30,
            sa.ratings_sum_4w AS rating_count_d30,
            csapc.sdk,
            csapc.api_call,
            csapc.app_ads_direct
           FROM ((adtech.combined_store_apps_parent_companies csapc
             LEFT JOIN frontend.store_apps_overview sa ON ((csapc.store_app = sa.id)))
             LEFT JOIN adtech.companies c ON ((csapc.company_id = c.id)))
          WHERE (csapc.sdk OR csapc.api_call OR csapc.app_ads_direct)
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
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
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
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
  WHERE (app_company_category_rank <= 20)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_parent_top_apps OWNER TO postgres;

--
-- Name: company_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_top_apps AS
 WITH deduped_data AS (
         SELECT cac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            cac.app_category,
            sa.installs_sum_4w AS installs_d30,
            sa.ratings_sum_4w AS rating_count_d30,
            cac.sdk,
            cac.api_call,
            cac.app_ads_direct
           FROM ((adtech.combined_store_apps_companies cac
             LEFT JOIN frontend.store_apps_overview sa ON ((cac.store_app = sa.id)))
             LEFT JOIN adtech.companies c ON ((cac.company_id = c.id)))
          WHERE (cac.app_ads_direct OR cac.sdk OR cac.api_call)
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
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
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
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
  WHERE (app_company_category_rank <= 20)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_top_apps OWNER TO postgres;

--
-- Name: keyword_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.keyword_scores AS
 WITH latest_en_descriptions AS (
         SELECT DISTINCT ON (sad.store_app) sad.store_app,
            sad.id AS description_id
           FROM (public.store_apps_descriptions sad
             JOIN public.description_keywords dk ON ((sad.id = dk.description_id)))
          WHERE (sad.language_id = 1)
          ORDER BY sad.store_app, sad.updated_at DESC
        ), keyword_app_counts AS (
         SELECT sa.store,
            k.keyword_text,
            dk.keyword_id,
            count(DISTINCT led.store_app) AS app_count
           FROM (((latest_en_descriptions led
             LEFT JOIN public.description_keywords dk ON ((led.description_id = dk.description_id)))
             LEFT JOIN public.keywords k ON ((dk.keyword_id = k.id)))
             LEFT JOIN public.store_apps sa ON ((led.store_app = sa.id)))
          WHERE (dk.keyword_id IS NOT NULL)
          GROUP BY sa.store, k.keyword_text, dk.keyword_id
        ), total_app_count AS (
         SELECT sa.store,
            count(*) AS total_apps
           FROM (latest_en_descriptions led
             LEFT JOIN public.store_apps sa ON ((led.store_app = sa.id)))
          GROUP BY sa.store
        )
 SELECT kac.store,
    kac.keyword_text,
    kac.keyword_id,
    kac.app_count,
    tac.total_apps,
    round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2) AS competitiveness_score
   FROM (keyword_app_counts kac
     LEFT JOIN total_app_count tac ON ((kac.store = tac.store)))
  ORDER BY (round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.keyword_scores OWNER TO postgres;

--
-- Name: latest_sdk_scanned_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.latest_sdk_scanned_apps AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at,
            version_codes.crawl_result
           FROM public.version_codes
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), ranked_apps AS (
         SELECT lvc.updated_at AS sdk_crawled_at,
            lvc.version_code,
            lvc.crawl_result,
            sa.store,
            sa.store_id,
            sa.name,
            sa.installs,
            sa.rating_count,
            row_number() OVER (PARTITION BY sa.store, lvc.crawl_result ORDER BY lvc.updated_at DESC) AS updated_rank
           FROM (latest_version_codes lvc
             LEFT JOIN public.store_apps sa ON ((lvc.store_app = sa.id)))
          WHERE (lvc.updated_at <= (CURRENT_DATE - '1 day'::interval))
        )
 SELECT sdk_crawled_at,
    version_code,
    crawl_result,
    store,
    store_id,
    name,
    installs,
    rating_count,
    updated_rank
   FROM ranked_apps ra
  WHERE (updated_rank <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.latest_sdk_scanned_apps OWNER TO postgres;

--
-- Name: store_app_api_companies; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_api_companies AS
 WITH latest_run_per_app AS (
         SELECT DISTINCT ON (saac_1.store_app) saac_1.store_app,
            saac_1.run_id
           FROM (public.store_app_api_calls saac_1
             JOIN public.version_code_api_scan_results vcasr ON ((saac_1.run_id = vcasr.id)))
          ORDER BY saac_1.store_app, vcasr.run_at DESC
        )
 SELECT DISTINCT sa.store_id,
    saac.tld_url AS company_domain,
    c.id AS company_id,
    c.name AS company_name,
    co.alpha2 AS country
   FROM (((((((((latest_run_per_app lrpa
     LEFT JOIN public.store_app_api_calls saac ON ((lrpa.run_id = saac.run_id)))
     LEFT JOIN public.ad_domains ad ON ((saac.tld_url = (ad.domain)::text)))
     LEFT JOIN public.store_apps sa ON ((saac.store_app = sa.id)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ad.id = cdm.domain_id)))
     LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
     LEFT JOIN public.ad_domains cad ON ((c.domain_id = cad.id)))
     LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
     LEFT JOIN public.ad_domains pcad ON ((pc.domain_id = pcad.id)))
     LEFT JOIN public.countries co ON ((saac.country_id = co.id)))
  ORDER BY sa.store_id DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_api_companies OWNER TO postgres;

--
-- Name: store_app_ranks_weekly; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.store_app_ranks_weekly (
    rank smallint NOT NULL,
    best_rank smallint NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    crawled_date date NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE frontend.store_app_ranks_weekly OWNER TO postgres;

--
-- Name: store_apps_z_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_z_scores AS
 WITH ranked_z_scores AS (
         SELECT saz.store_app,
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
            row_number() OVER (PARTITION BY sa.store, cm.mapped_category,
                CASE
                    WHEN (sa.store = 2) THEN 'rating'::text
                    ELSE 'installs'::text
                END ORDER BY
                CASE
                    WHEN (sa.store = 2) THEN saz.ratings_z_score_2w
                    WHEN (sa.store = 1) THEN saz.installs_z_score_2w
                    ELSE NULL::numeric
                END DESC NULLS LAST) AS rn
           FROM ((public.store_app_z_scores saz
             LEFT JOIN public.store_apps sa ON ((saz.store_app = sa.id)))
             LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
        )
 SELECT store,
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
-- Name: total_categories_app_counts; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.total_categories_app_counts AS
 SELECT sa.store,
    tag.tag_source,
    csac.app_category,
    count(DISTINCT csac.store_app) AS app_count
   FROM ((adtech.combined_store_apps_companies csac
     LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
     CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
  WHERE (tag.present IS TRUE)
  GROUP BY sa.store, tag.tag_source, csac.app_category
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.total_categories_app_counts OWNER TO postgres;

--
-- PostgreSQL database dump complete
--


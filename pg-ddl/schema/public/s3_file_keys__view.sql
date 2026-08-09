--
-- PostgreSQL database dump
--

\restrict OmvNeGBJbam5npgwonlRCeIBzUl9jO90jaVwylyXFokVocnyd6bvIwDenBSKYhZ

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
-- Name: s3_file_keys; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.s3_file_keys AS
 WITH files_combined AS (
         SELECT s3_package_inventory_hot.store_app,
            s3_package_inventory_hot.version_code_id,
            s3_package_inventory_hot.myregion,
            s3_package_inventory_hot.file_key,
            0 AS priority
           FROM public.s3_package_inventory_hot
          WHERE ((s3_package_inventory_hot.inserted_at >= (now() - '24:00:00'::interval)) AND (s3_package_inventory_hot.store_app IS NOT NULL) AND (s3_package_inventory_hot.version_code_id IS NOT NULL))
        UNION ALL
         SELECT s3_package_inventory.store_app,
            s3_package_inventory.version_code_id,
            s3_package_inventory.myregion,
            s3_package_inventory.file_key,
            1 AS priority
           FROM public.s3_package_inventory
          WHERE ((s3_package_inventory.store_app IS NOT NULL) AND (s3_package_inventory.version_code_id IS NOT NULL))
        )
 SELECT DISTINCT ON (store_app, version_code_id) store_app,
    version_code_id,
    myregion,
    file_key
   FROM files_combined
  ORDER BY store_app, version_code_id, priority,
        CASE
            WHEN (myregion = 'loki'::text) THEN 0
            ELSE 1
        END;


ALTER VIEW public.s3_file_keys OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict OmvNeGBJbam5npgwonlRCeIBzUl9jO90jaVwylyXFokVocnyd6bvIwDenBSKYhZ


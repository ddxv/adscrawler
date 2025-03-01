-- public.publisher_network_view source

CREATE MATERIALIZED VIEW public.publisher_network_view
TABLESPACE pg_default
AS SELECT DISTINCT
    av.developer_domain_url AS publisher_domain_url,
    av.ad_domain_url,
    av.relationship
FROM app_ads_view AS av
WHERE av.developer_domain_crawled_at::date = av.txt_entry_crawled_at::date
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX publisher_network_view_uniq_idx
ON public.publisher_network_view USING btree (
    publisher_domain_url, ad_domain_url, relationship
);



CREATE MATERIALIZED VIEW category_mapping AS
SELECT DISTINCT
    original_category,
    CASE
        WHEN
            mapped_category IN (
                'action',
                'casual',
                'adventure',
                'arcade',
                'board',
                'card',
                'casino',
                'puzzle',
                'racing',
                'simulation',
                'strategy',
                'trivia',
                'word'
            )
            THEN 'game_' || mapped_category
        WHEN mapped_category = 'news_and_magazines' THEN 'news'
        WHEN mapped_category = 'educational' THEN 'education'
        WHEN mapped_category = 'book' THEN 'books_and_reference'
        WHEN mapped_category = 'navigation' THEN 'maps_and_navigation'
        WHEN mapped_category = 'music' THEN 'music_and_audio'
        WHEN mapped_category = 'photography' THEN 'photo_and_video'
        WHEN mapped_category = 'reference' THEN 'books_and_reference'
        WHEN mapped_category = 'role playing' THEN 'game_role_playing'
        WHEN mapped_category = 'social' THEN 'social networking'
        WHEN mapped_category = 'travel' THEN 'travel_and_local'
        WHEN mapped_category = 'utilities' THEN 'tools'
        WHEN mapped_category = 'video players_and_editors' THEN 'video_players'
        WHEN mapped_category = 'graphics_and_design' THEN 'art_and_design'
        WHEN mapped_category = 'parenting' THEN 'family'
        WHEN mapped_category IS NULL THEN 'N/A'
        ELSE mapped_category
    END AS mapped_category
FROM
    (
        SELECT DISTINCT
            category AS original_category,
            regexp_replace(
                lower(category),
                ' & ',
                '_and_'
            ) AS mapped_category
        FROM
            store_apps
    ) AS sub;
-- Create indexes:
CREATE UNIQUE INDEX category_mapping_idx
ON public.category_mapping USING btree (
    original_category, mapped_category
);

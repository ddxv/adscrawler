INSERT INTO public.crawl_results (outcome) VALUES
	 ('success'),
	 ('parse_failure'),
	 ('404'),
	 ('to_be_deleted'),
	 ('unknown');


INSERT INTO public.platforms ("name") VALUES
	 ('android'),
	 ('ios');

INSERT INTO public.stores ("name",platform) VALUES
	 ('google',1),
	 ('itunes',2);



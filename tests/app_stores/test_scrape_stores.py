import unittest
from unittest.mock import patch

import pandas as pd

from adscrawler.app_stores.process_icons import build_icon_update_df
from adscrawler.app_stores.scrape_stores import extract_domains_with_sub, save_app_ranks


class TestBuildIconUpdateDf(unittest.TestCase):
    @patch("adscrawler.app_stores.process_icons.process_app_icon")
    def test_build_icon_update_df_populates_missing_variants(
        self, mock_process_app_icon
    ):
        mock_process_app_icon.return_value = ("app_128.png", "app_64.png")
        apps_df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "store_id": "com.example.one",
                    "icon_url_512": "https://x/1.png",
                },
                {
                    "id": 2,
                    "store_id": "com.example.two",
                    "icon_url_512": "https://x/2.png",
                },
                {"id": 3, "store_id": "com.example.three", "icon_url_512": None},
            ]
        )

        result = build_icon_update_df(apps_df)

        self.assertEqual(result.shape[0], 2)
        self.assertEqual(result.loc[0, "icon_128"], "app_128.png")
        self.assertEqual(result.loc[0, "icon_64"], "app_64.png")
        self.assertEqual(result.loc[0, "id"], 1)
        self.assertEqual(result.iloc[1]["id"], 2)


class TestExtractDomains(unittest.TestCase):
    def test_extract_domains_hardcoded_values(self) -> None:
        cases = [
            ("https://www.example.com/path/to/page", "example.com"),
            ("http://google.com", "google.com"),
            ("https://openai.com/chat", "openai.com"),
            (None, None),
            (float("nan"), None),
            ("", "."),
            ("https://github.com", "github.com"),
            ("https://192.168.1.1/admin", "192.168.1.1."),
            ("10.0.0.138", "10.0.0.138."),
            ("https://www.python.org/downloads", "python.org"),
            ("not-a-url", "not-a-url."),
            ("https://x.ai", "x.ai"),
            ("https://en.wikipedia.org/wiki/Main_Page", "en.wikipedia.org"),
            (None, None),
            ("", "."),
            ("https://api.stripe.com/v1/charges", "api.stripe.com"),
            ("https://localhost:3000", "localhost."),
            ("http://256.256.256.256", "256.256.256.256."),
            ("https://example.com", "example.com"),
            ("htp://invalid-scheme.com", "invalid-scheme.com"),
        ]

        for value, expected in cases:
            with self.subTest(value=value):
                result = extract_domains_with_sub(value)
                self.assertEqual(result, expected)


class TestSaveAppRanks(unittest.TestCase):
    @patch("adscrawler.app_stores.scrape_stores.process_store_rankings")
    @patch("adscrawler.app_stores.scrape_stores.query_store_id_map")
    def test_crawl_timestamp_is_added_or_preserved(self, query_map, upload):
        query_map.return_value = pd.DataFrame(
            [{"store": 1, "store_id": "app", "id": 5}]
        )
        source = pd.DataFrame([{"store": 1, "store_id": "app", "country": "us"}])
        save_app_ranks(source, object(), 1, pd.DataFrame(), pd.DataFrame())
        uploaded = upload.call_args.kwargs["df"]
        self.assertEqual(str(uploaded.loc[0, "crawled_at"].tz), "UTC")
        self.assertNotIn("crawled_at", source.columns)

        source["crawled_at"] = pd.Timestamp("2025-01-01T12:00:00Z")
        save_app_ranks(source, object(), 1, pd.DataFrame(), pd.DataFrame())
        self.assertEqual(
            upload.call_args.kwargs["df"].loc[0, "crawled_at"],
            source.loc[0, "crawled_at"],
        )


if __name__ == "__main__":
    unittest.main()

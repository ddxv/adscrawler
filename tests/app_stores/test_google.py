import unittest
from unittest.mock import patch

from adscrawler.app_stores.google import search_play_store


class TestGoogleSearch(unittest.TestCase):
    @patch("adscrawler.app_stores.google.appgoblin_play_scraper.search")
    def test_search_play_store_normalizes_raw_results(self, mock_search) -> None:
        mock_search.return_value = [
            {
                "appId": None,
                "title": "YouTube",
                "developer": "Google LLC",
                "icon": "https://example.com/icon0.png",
            },
            {
                "appId": "com.example.app",
                "title": "Example App",
                "url": "https://play.google.com/store/apps/details?id=com.example.app",
                "developer": "Example Dev",
                "icon": "https://example.com/icon1.png",
                "score": 4.5,
            },
        ]

        results = search_play_store("example", country="us", language="en")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["store_id"], "com.example.app")
        self.assertEqual(results[0]["id"], "com.example.app")
        self.assertEqual(
            results[0]["store_link"],
            "https://play.google.com/store/apps/details?id=com.example.app",
        )
        self.assertEqual(results[0]["name"], "Example App")
        self.assertEqual(results[0]["developer_name"], "Example Dev")
        self.assertEqual(results[0]["icon_url_512"], "https://example.com/icon1.png")
        self.assertEqual(results[0]["store"], 1)
        self.assertEqual(results[0]["country"], "us")
        self.assertEqual(results[0]["language"], "en")
        self.assertNotIn("appId", results[0])
        self.assertNotIn("title", results[0])
        self.assertNotIn("url", results[0])
        self.assertNotIn("developer", results[0])
        self.assertNotIn("icon", results[0])

    @patch("adscrawler.app_stores.google.appgoblin_play_scraper.search")
    def test_search_play_store_builds_store_link_when_missing(
        self, mock_search
    ) -> None:
        mock_search.return_value = [
            {
                "appId": "com.example.app",
                "title": "Example App",
                "developer": "Example Dev",
                "icon": "https://example.com/icon1.png",
            }
        ]

        results = search_play_store("example", country="mx", language="es")

        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]["store_link"],
            "https://play.google.com/store/apps/details?id=com.example.app",
        )


if __name__ == "__main__":
    unittest.main()

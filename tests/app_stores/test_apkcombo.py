import unittest
from typing import Any
from unittest.mock import patch

from adscrawler.app_stores import apkcombo


class TestApkcombo(unittest.TestCase):
    # Mock the scrape_with_firefox function to return a sample list
    @patch("adscrawler.app_stores.apkcombo.scrape_with_firefox")
    def test_get_apkcombo_android_apps(self, mock_scrape: Any) -> None:
        mock_scrape.return_value = ["sample_app_id_1", "sample_app_id_2"]

        # Call the function
        result = apkcombo.get_apkcombo_android_apps()

        # Assert that the returned list has length greater than 0
        self.assertTrue(len(result) > 0)


if __name__ == "__main__":
    unittest.main()

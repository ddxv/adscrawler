import os
import unittest
from unittest.mock import Mock, patch

from adscrawler.app_stores import apkcombo


class TestApkcombo(unittest.TestCase):
    @patch("adscrawler.app_stores.apkcombo.webdriver.Firefox")
    def test_scrape_with_firefox_quits_driver_after_success(
        self,
        mock_firefox: Mock,
    ) -> None:
        driver = mock_firefox.return_value
        driver.page_source = '<guid isPermaLink="false">sample-app</guid>'

        apkcombo.scrape_with_firefox()

        driver.quit.assert_called_once()

    @patch("adscrawler.app_stores.apkcombo.webdriver.Firefox")
    def test_scrape_with_firefox_quits_driver_after_failure(
        self,
        mock_firefox: Mock,
    ) -> None:
        driver = mock_firefox.return_value
        driver.get.side_effect = RuntimeError("fetch failed")

        with self.assertRaises(RuntimeError):
            apkcombo.scrape_with_firefox()

        driver.quit.assert_called_once()

    def test_get_apkcombo_android_apps(self) -> None:
        use_mock = os.environ.get("USE_MOCK", "true").lower() == "true"

        if use_mock:
            with patch(
                "adscrawler.app_stores.apkcombo.scrape_with_firefox",
            ) as mock_scrape:
                mock_scrape.return_value = ["sample_app_id_1", "sample_app_id_2"]
                # Call the function
                results = apkcombo.get_apkcombo_android_apps()
        else:
            # Call the function without mocking
            results = apkcombo.get_apkcombo_android_apps()

        # Assert that the returned list has length greater than 0
        self.assertTrue(len(results) > 0)


if __name__ == "__main__":
    unittest.main()

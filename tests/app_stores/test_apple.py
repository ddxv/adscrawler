import unittest

import pandas as pd
from bs4 import BeautifulSoup

from adscrawler.app_stores.apple import clean_ios_app_df, is_apple_tv_supported


def test_clean_ios_app_df_only_removes_actual_float_suffix():
    df = pd.DataFrame(
        {
            "trackId": ["1", "2", "3"],
            "artistId": ["123.0", "102345", "123.01.0"],
            "primaryGenreName": ["Utilities"] * 3,
            "genres": ["Utilities"] * 3,
            "price": [0] * 3,
            "currentVersionReleaseDate": ["2026-01-01"] * 3,
            "releaseDate": ["2026-01-01T00:00:00Z"] * 3,
            "screenshotUrls": [None] * 3,
            "ipadScreenshotUrls": [None] * 3,
            "user_ratings": [""] * 3,
            "languageCodesISO2A": ["en"] * 3,
        }
    )
    assert clean_ios_app_df(df)["developer_id"].tolist() == [
        "123", "102345", "123.01"
    ]


class TestAppleTvSupport(unittest.TestCase):
    def test_json_ld_available_on_device(self):
        soup = BeautifulSoup(
            """
            <script type="application/ld+json">
            {"@context":"https://schema.org", "availableOnDevice":"Apple TV, iPhone, iPad"}
            </script>
            """,
            "html.parser",
        )

        self.assertTrue(is_apple_tv_supported(soup))

    def test_visible_platform_list(self):
        soup = BeautifulSoup(
            '<div class="all-platforms-names svelte-j9g7ud">'
            "iPhone, iPad, Apple Vision, Apple TV</div>",
            "html.parser",
        )

        self.assertTrue(is_apple_tv_supported(soup))

    def test_non_tv_platforms_are_not_supported(self):
        soup = BeautifulSoup(
            '<div class="all-platforms-names">iPhone, iPad</div>',
            "html.parser",
        )

        self.assertFalse(is_apple_tv_supported(soup))


if __name__ == "__main__":
    unittest.main()

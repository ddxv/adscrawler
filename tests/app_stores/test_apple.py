import unittest

from bs4 import BeautifulSoup

from adscrawler.app_stores.apple import is_apple_tv_supported


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

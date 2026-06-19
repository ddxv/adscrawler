import unittest

from adscrawler.app_stores.scrape_stores import extract_domains_with_sub


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


if __name__ == "__main__":
    unittest.main()

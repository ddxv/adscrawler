import unittest

from adscrawler.mitm_ad_parser.network_parsers import (
    extract_and_decode_urls,
    strip_trailing_encoded_url_delimiters,
)


class TestExtractAndDecodeUrls(unittest.TestCase):
    def test_preserves_encoded_brackets_inside_query_values(self) -> None:
        text = (
            "https://googleads.g.doubleclick.net/dbm/clk?client="
            "ca-app-pub-1515339944588980&fbs_aeid=%5Bgw_fbsaeid%5D&dbm_c="
            "AKAmf-CCJugGGMNJrGIej1PgCEfs0Zuo299w6zTLKGtSpxlfBGzRf6qjxUItMlAs"
            "2nDiLiemJD6nFwhuCuPFiAy3fthhzbVu-Ms4eGcw1hj2WN8KdqR8GmE0TE9chHPe"
            "BGtQiSNeZDcmoAQLDtCRBl6Ww3-u_N1ie9TlxcxDNgXdgfVNQ-LVTTZXgNz3L6KY"
            "5HwHRUTSaBLxaquDwNwTYpjH4Wl7H7wRXb_pPpussUo_BtrdssI6T_aKMNR2cy85"
            "OlQEh0ADF9m-yZhjrJnQoAuI1pIs7Lc0WGoK2RIXHTe8hKMjlM0fzWg75AoKi9Y4"
            "08Dh2t280vLtu0Y45KRHDTP4N2Nb0cy587zkTrO3SM2rMMAkVV5f7CsDy8NIKmsV"
            "nTTtj9rCQ95K_mHsPRVmMyfCZ0_HO9_DGVyE62FFjKoynI08tXy0U8DNnwK2Jv9E"
            "gjAFydg8Iyv5dY3qxlu__LEhxWdZf39E9g&adurl=https://play.google.com/st"
            "ore/apps/details?id=com.supercell.clashofclans"
        )

        urls = extract_and_decode_urls(text)

        doubleclick_urls = [
            url for url in urls if "googleads.g.doubleclick.net/dbm/clk" in url
        ]

        self.assertEqual(len(doubleclick_urls), 1)
        self.assertIn("fbs_aeid=[gw_fbsaeid]", doubleclick_urls[0])
        self.assertIn(
            "dbm_c=AKAmf-CCJugGGMNJrGIej1PgCEfs0Zuo299w6zTLK",
            doubleclick_urls[0],
        )
        self.assertIn(
            "adurl=https://play.google.com/store/apps/details?id=com.supercell.clashofclans",
            doubleclick_urls[0],
        )
        self.assertIn(
            "https://play.google.com/store/apps/details?id=com.supercell.clashofclans",
            urls,
        )

    def test_trims_only_unmatched_trailing_encoded_delimiters(self) -> None:
        self.assertEqual(
            strip_trailing_encoded_url_delimiters(
                "https://example.com/path?x=%5Babc%5D"
            ),
            "https://example.com/path?x=%5Babc%5D",
        )
        self.assertEqual(
            strip_trailing_encoded_url_delimiters(
                "https://example.com/path?x=%5Babc%5D%5D"
            ),
            "https://example.com/path?x=%5Babc%5D",
        )
        self.assertEqual(
            strip_trailing_encoded_url_delimiters("https://example.com/path%5d"),
            "https://example.com/path",
        )
        self.assertEqual(
            strip_trailing_encoded_url_delimiters(
                "https://example.com/path?x=%5babc%5d&y=1"
            ),
            "https://example.com/path?x=%5babc%5d&y=1",
        )

    def test_extracts_nested_play_store_url_from_tracker_query(self) -> None:
        text = (
            "foo https://tracker.example/click?redirect=https%3A%2F%2Fplay.google.com"
            "%2Fstore%2Fapps%2Fdetails%3Fid%3Dcom.supercell.clashofclans bar"
        )

        urls = extract_and_decode_urls(text)

        self.assertIn(
            "https://tracker.example/click?redirect=https://play.google.com/store/apps/details?id=com.supercell.clashofclans",
            urls,
        )
        self.assertIn(
            "https://play.google.com/store/apps/details?id=com.supercell.clashofclans",
            urls,
        )

    def test_preserves_outer_encoded_url_when_decoding_would_introduce_angle_junk(
        self,
    ) -> None:
        text = '"https://example.com/click?url=https%3A%2F%2Fad.com%3Egarbage"'

        urls = extract_and_decode_urls(text)

        self.assertIn(
            "https://example.com/click?url=https%3A%2F%2Fad.com%3Egarbage",
            urls,
        )
        self.assertIn("https://ad.com", urls)
        self.assertNotIn("https://example.com/click?url=https://ad.com", urls)
        self.assertNotIn("https://example.com/click?url=https://ad.com>garbage", urls)

    def test_preserves_outer_encoded_url_when_decoding_would_introduce_unmatched_bracket(
        self,
    ) -> None:
        text = 'href="https://example.com/click?x=https%3A%2F%2Fad.com%5Bslot%5D"'

        urls = extract_and_decode_urls(text)

        self.assertIn(
            "https://example.com/click?x=https://ad.com[slot]",
            urls,
        )
        self.assertIn("https://ad.com", urls)
        self.assertNotIn("https://ad.com[slot]", urls)
        self.assertNotIn("https://example.com/click?x=https://ad.com[slot", urls)
        self.assertNotIn("https://ad.com[slot", urls)

    def test_extracts_top_level_double_encoded_url(self) -> None:
        text = "redirect=https%3A%2F%2Fmiddle.com%3Furl%3Dhttps%253A%252F%252Finner.com"

        urls = extract_and_decode_urls(text)

        self.assertIn("https://middle.com?url=https%3A%2F%2Finner.com", urls)
        self.assertIn("https://inner.com", urls)


if __name__ == "__main__":
    unittest.main()

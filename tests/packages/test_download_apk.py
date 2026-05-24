import unittest
from unittest.mock import patch

from adscrawler.packages.apks.download_apk import (
    ExistingOrOlderVersionError,
    external_download,
    gplaydl_download,
    manage_apk_download,
)


class TestDownloadApk(unittest.TestCase):
    def test_gplaydl_download_raises_when_version_not_newer(self) -> None:
        info_output = """
                               CrazyGames: Play 2500+ Games
┌────────────┬────────────────────────────────────────────────────────────────────────────┐
│ Package    │ com.crazygames.crazygamesapp                                               │
│ Version    │ 1.5.6 (33)                                                                 │
│ Developer  │ CrazyGames                                                                 │
└────────────┴────────────────────────────────────────────────────────────────────────────┘
"""
        with patch("adscrawler.packages.apks.download_apk.subprocess.run") as mock_run:
            mock_run.return_value.stdout = info_output

            with self.assertRaises(ExistingOrOlderVersionError):
                gplaydl_download(
                    "com.crazygames.crazygamesapp",
                    last_downloaded_version_code="33",
                )

        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        self.assertEqual(args[0][-2:], ["info", "com.crazygames.crazygamesapp"])
        self.assertTrue(kwargs["capture_output"])
        self.assertTrue(kwargs["text"])

    def test_external_download_does_not_fallback_after_stale_version(self) -> None:
        with (
            patch(
                "adscrawler.packages.apks.download_apk.APK_SOURCES",
                ["gplaydl", "apkpure"],
            ),
            patch(
                "adscrawler.packages.apks.download_apk.gplaydl_download",
                side_effect=ExistingOrOlderVersionError(
                    "com.crazygames.crazygamesapp", 33, 33
                ),
            ),
            patch(
                "adscrawler.packages.apks.download_apk.get_download_url"
            ) as mock_get_download_url,
        ):
            with self.assertRaises(ExistingOrOlderVersionError):
                external_download(
                    "com.crazygames.crazygamesapp",
                    last_downloaded_version_code="33",
                )

        mock_get_download_url.assert_not_called()

    def test_manage_apk_download_marks_stale_version_as_non_error(self) -> None:
        with patch(
            "adscrawler.packages.apks.download_apk.external_download",
            side_effect=ExistingOrOlderVersionError(
                "com.crazygames.crazygamesapp", 33, 33
            ),
        ):
            result = manage_apk_download(
                "com.crazygames.crazygamesapp",
                last_downloaded_version_code="33",
            )

        self.assertEqual(result.crawl_result, 1)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(result.version_str, "33")
        self.assertIsNone(result.downloaded_file_path)


if __name__ == "__main__":
    unittest.main()

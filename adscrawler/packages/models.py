import pathlib


class DownloadResult:
    def __init__(
        self,
        crawl_result: int,
        error_count: int,
        version_str: str | None = None,
        md5_hash: str | None = None,
        downloaded_file_path: pathlib.Path | None = None,
    ):
        self.crawl_result = crawl_result
        self.version_str = version_str
        self.md5_hash = md5_hash
        self.downloaded_file_path = downloaded_file_path
        self.error_count = error_count

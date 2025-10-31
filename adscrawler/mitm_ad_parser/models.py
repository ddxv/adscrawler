import dataclasses
from typing import Any, Self

import tldextract


@dataclasses.dataclass
class AdInfo:
    adv_store_id: str | None
    host_ad_network_tld: str | None = None
    init_tld: str | None = None
    found_ad_network_tlds: list[str] | None = None
    found_mmp_urls: list[str] | None = None

    def __getitem__(self: Self, key: str) -> Any:
        """Support dictionary-style access to dataclass fields."""
        return getattr(self, key)

    def __setitem__(self: Self, key: str, value: Any) -> None:
        """Support dictionary-style assignment to dataclass fields."""
        setattr(self, key, value)

    @property
    def mmp_tld(self: Self) -> str | None:
        if self.found_mmp_urls and len(self.found_mmp_urls) > 0:
            return (
                tldextract.extract(self.found_mmp_urls[0]).domain
                + "."
                + tldextract.extract(self.found_mmp_urls[0]).suffix
            )
        return None


class MultipleAdvertiserIdError(Exception):
    """Raised when multiple advertiser store IDs are found for the same ad."""

    def __init__(self: Self, found_adv_store_ids: list[str]) -> None:
        self.found_adv_store_ids = found_adv_store_ids
        super().__init__(f"multiple adv_store_id found for {found_adv_store_ids}")

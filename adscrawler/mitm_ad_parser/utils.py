import tldextract


def get_tld(url: str) -> str | None:
    """Extracts the top-level domain from a URL."""
    tld = tldextract.extract(url)
    if not tld.suffix:
        return None
    tld_url = tld.domain + "." + tld.suffix
    if tld_url == ".":
        tld_url = None
    return tld_url

import tldextract


def get_tld(url: str) -> str:
    """Extracts the top-level domain from a URL."""
    tld = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
    return tld

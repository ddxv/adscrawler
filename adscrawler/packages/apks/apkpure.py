APKPURE_URL = "https://d.apkpure.net/b/XAPK/{store_id}?version=latest"


def get_download_url(store_id: str) -> str:
    """Get the download url for the apk."""
    return APKPURE_URL.format(store_id=store_id)

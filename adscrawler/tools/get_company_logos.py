"""
fetch_logos_simple.py

Usage:
    python fetch_logos_simple.py

Outputs:
 - logos/<domain>/logo.* (downloaded chosen image)
 - logos_mapping.csv with domain, chosen_path, chosen_url
"""

import os
import pathlib
import re
from io import BytesIO
from time import sleep
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from PIL import Image

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import query_companies, update_company_logo_url
from adscrawler.packages.storage import get_s3_client

logger = get_logger(__name__)

OUTPUT_DIR = "/tmp/company-logos"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}


ICON_REL_PAT = re.compile(r"icon|apple-touch-icon|shortcut|mask-icon", re.I)
LOGO_IMG_PAT = re.compile(r"logo|brand|icon", re.I)


def normalize_url(domain_or_url: str) -> str:
    domain_or_url = domain_or_url.strip()
    if domain_or_url.startswith("http"):
        return domain_or_url
    return "https://" + domain_or_url


def domain_from_url(url: str) -> str:
    return urlparse(url).netloc or url


FAVICON_FILENAMES = [
    "/favicon.ico",
    "/favicon.png",
    "/apple-touch-icon.png",
    "/apple-touch-icon-precomposed.png",
    "/android-chrome-192x192.png",
    "/android-chrome-512x512.png",
]


def guess_candidates(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    # --- icons explicitly in HTML ---
    for link in soup.find_all("link", href=True):
        rel = " ".join(link.get("rel") or [])
        if ICON_REL_PAT.search(rel):
            candidates.append(link["href"])

    # --- explicit favicon guesses ---
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    for fname in FAVICON_FILENAMES:
        candidates.append(root + fname)

    # --- meta og:image etc. (fallback only) ---
    for meta_name in ("og:image", "twitter:image"):
        tag = soup.find("meta", attrs={"property": meta_name}) or soup.find(
            "meta", attrs={"name": meta_name}
        )
        if tag and tag.get("content"):
            candidates.append(tag["content"])

    # --- logo-looking <img> ---
    for img in soup.find_all("img", src=True):
        src = img["src"]
        alt = img.get("alt", "")
        if LOGO_IMG_PAT.search(src) or LOGO_IMG_PAT.search(alt):
            candidates.append(src)

    # resolve URLs
    out = []
    for c in candidates:
        if not c:
            continue
        if c.startswith("//"):
            c = parsed.scheme + ":" + c
        if not c.startswith("http"):
            c = urljoin(base_url, c)
        if c not in out:
            out.append(c)
    return out


def fetch_image(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200 and len(r.content) > 200:
            return r.content
    except Exception:
        return None
    return None


def pick_best(images: list[tuple[str, bytes]]) -> tuple[str, bytes] | None:
    best = None
    best_score = -1
    for url, data in images:
        try:
            im = Image.open(BytesIO(data))
            w, h = im.size
            area = w * h
            if w == 0 or h == 0:
                continue
            aspect_ratio = max(w, h) / min(w, h)

            # Scoring: prefer square-ish, then larger size
            score = area
            if 0.9 <= aspect_ratio <= 1.1:  # ~square
                print(f"square: {url}")
                score *= 3  # triple weight if square

            if score > best_score:
                best = (f"logo_{w}x{h}", data)
                best_score = score
        except Exception:
            continue
    return best


def find_linkedin_url(html: str) -> list[str]:
    linkedin_urls = []
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        if "linkedin.com" in link["href"]:
            linkedin_urls.append(link["href"])
    return list(set(linkedin_urls))


def process_site(domain: str) -> str | None:
    try:
        r = requests.get(f"https://{domain}", headers=HEADERS, timeout=10)
        if r.status_code != 200:
            logger.error(f"Failed to get {domain}")
            return None
    except Exception:
        logger.error(f"Failed to get {domain}")
        return None
    linkedin_urls = find_linkedin_url(r.text)
    print(linkedin_urls)
    if not linkedin_urls:
        return None
    for linkedin_url in linkedin_urls:
        l_r = requests.get(linkedin_url, headers=HEADERS, timeout=10)
        candidates = parse_linkedin(html=l_r.text)
    images = []
    for c in candidates:
        data = fetch_image(c)
        if data:
            images.append((c, data))

    best = pick_best(images)
    if not best:
        return None

    filename, data = best
    # detect format
    try:
        im = Image.open(BytesIO(data))
        fmt = im.format.lower() if im.format else "png"
    except Exception:
        fmt = "png"

    filename = filename + "." + fmt
    save_dir = os.path.join(OUTPUT_DIR, domain)
    os.makedirs(save_dir, exist_ok=True)
    path = os.path.join(save_dir, filename)
    with open(path, "wb") as f:
        f.write(data)
    return filename


def parse_linkedin(html: str) -> list[str]:
    logo_urls = []
    # --- meta og:image etc. (fallback only) ---
    soup = BeautifulSoup(html, "html.parser")
    for meta_name in ("og:image", "twitter:image"):
        tag = soup.find("meta", attrs={"property": meta_name}) or soup.find(
            "meta", attrs={"name": meta_name}
        )
        if tag and tag.get("content"):
            logo_urls.append(tag["content"])
    return list(set(logo_urls))


def check_logo_exists_s3(domain: str) -> str | None:
    filename = None
    if not os.path.exists(os.path.join(OUTPUT_DIR, domain)):
        os.makedirs(os.path.join(OUTPUT_DIR, domain), exist_ok=True)
    s3_client = get_s3_client("digi-cloud")
    response = s3_client.list_objects_v2(
        Bucket=CONFIG["digi-cloud"]["bucket"], Prefix=f"company-logos/{domain}/"
    )
    if response["KeyCount"] == 0:
        return None
    for obj in response["Contents"]:
        filename = obj["Key"].split("/")[-1]
        if filename.startswith("logo_"):
            return str(filename)
    return None


def upload_company_logo_to_s3(
    domain: str,
    file_path: pathlib.Path,
) -> None:
    """Upload apk to s3."""
    filename = file_path.name
    file_format = file_path.suffix[1:]
    image_format = "image/" + file_format
    s3_client = get_s3_client("digi-cloud")
    response = s3_client.put_object(
        Bucket=CONFIG["digi-cloud"]["bucket"],
        Key=f"company-logos/{domain}/{filename}",
        ACL="public-read",
        Body=file_path.read_bytes(),
        ExtraArgs={"ContentType": image_format},
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"Uploaded {domain} logo to S3")
    else:
        logger.error(f"Failed to upload {domain} logo to S3")


def update_company_logos() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    use_ssh_tunnel = True
    database_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)

    companies = query_companies(database_connection=database_connection)

    for _, row in companies.iterrows():
        input_domain = row["company_domain"]
        company_id = row["company_id"]
        filename = check_logo_exists_s3(input_domain)
        if filename and row["company_logo_url"]:
            logger.info(f"{input_domain} logo already in S3")
            continue
        if filename and not row["company_logo_url"]:
            logger.info(f"Updating {input_domain} logo url to {filename}")
            logo_url = f"company-logos/{input_domain}/{filename}"
            update_company_logo_url(
                company_id=company_id,
                logo_url=logo_url,
                database_connection=database_connection,
            )
        else:
            logger.info(f"Processing: {input_domain}")
            filename = process_site(input_domain)
            if not filename:
                logger.info(f"No logo found for {input_domain}")
                continue
            upload_company_logo_to_s3(
                domain=input_domain,
                file_path=pathlib.Path(OUTPUT_DIR, input_domain, filename),
            )

            logger.info(f"Uploaded {input_domain}  to S3")
            logo_url = f"input_domain/{filename}"
            update_company_logo_url(
                company_id=company_id,
                logo_url=filename,
                database_connection=database_connection,
            )
            sleep(2)

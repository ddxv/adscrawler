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
from urllib.parse import urlparse

import requests
import tldextract
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
        return domain_or_url.replace("http://", "https://")
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


def try_guessing(domain: str) -> str | None:
    logger.info("Try Guessing LinkedIn")
    linkedin_base_url = "https://www.linkedin.com/company/"
    company_name = tldextract.extract(domain).domain
    l_r = requests.get(linkedin_base_url + company_name, headers=HEADERS, timeout=10)
    linkedin_candidates = parse_linkedin(html=l_r.text)
    if linkedin_candidates:
        file_name = process_candidates(linkedin_candidates, domain)
        if file_name:
            return file_name
    return None


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
                logger.info(f"found squre logo: {url}")
                score *= 3  # triple weight if square

            if score > best_score:
                best = (f"logo_{w}x{h}", data)
                best_score = score
        except Exception:
            continue
    return best


def find_other_domains(other_tld: str, html: str) -> list[str]:
    other_urls = []
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        if other_tld in link["href"]:
            other_urls.append(link["href"])
    return list(set(other_urls))


def parse_github(html: str) -> list[str]:
    github_urls = []
    soup = BeautifulSoup(html, "html.parser")
    try:
        header = soup.body.main.header  # only look inside <head>
    except Exception:
        return []
    if not header:
        return []
    for img in header.find_all("img", src=True):
        src = img["src"]
        alt = img.get("alt", "")
        if "avatar" in src or "avatar" in alt:
            github_urls.append(src)
    return list(set(github_urls))


def crawl_linked_in_urls(linkedin_urls: list[str]) -> list[str]:
    candidates = []
    if linkedin_urls and len(linkedin_urls) > 0:
        logger.info(f"Found {len(linkedin_urls)} linkedin urls")
        for linkedin_url in linkedin_urls:
            linkedin_url = normalize_url(linkedin_url)
            l_r = requests.get(linkedin_url, headers=HEADERS, timeout=10)
            linkedin_candidates = parse_linkedin(html=l_r.text)
            if linkedin_candidates:
                candidates += linkedin_candidates
    return candidates


def process_site(
    domain: str, check_domain: str, official_linkedin_url: str | None = None
) -> str | None:
    candidates = []
    if official_linkedin_url:
        linkedin_urls = [official_linkedin_url]
        candidates += crawl_linked_in_urls(linkedin_urls=linkedin_urls)
        if candidates:
            file_name = process_candidates(candidates, domain)
            if file_name:
                return file_name
    if "github.com-" in check_domain:
        check_domain = check_domain.replace("github.com-", "github.com/")
    try:
        logger.info(f"Try loading: {check_domain}")
        r = requests.get(f"https://{check_domain}", headers=HEADERS, timeout=10)
        if r.status_code != 200:
            logger.error(f"Failed to get {check_domain}")
            return None
    except Exception:
        logger.error(f"Failed to get {check_domain}")
        return None
    html = r.text
    if "github.com" in check_domain:
        logger.info("Found github.com in domain")
        candidates = parse_github(html=html)
    linkedin_urls = find_other_domains(other_tld="linkedin.com", html=html)
    github_urls = []
    if "github.com" not in check_domain:
        github_urls = find_other_domains(other_tld="github.com", html=html)
    candidates += crawl_linked_in_urls(linkedin_urls=linkedin_urls)
    if github_urls and len(github_urls) > 0:
        logger.info(f"Found {len(github_urls)} github urls")
        for github_url in github_urls:
            github_url = re.sub(r"(^.*\.?)github\.com", r"github.com", github_url)
            if "/" in github_url.split(".com/")[1]:
                github_url = "/".join(github_url.split("/")[:-1])
            github_url = "https://" + github_url
            g_r = requests.get(github_url, headers=HEADERS, timeout=10)
            github_candidates = parse_github(html=g_r.text)
            if github_candidates:
                candidates += github_candidates
    # instagram_urls = find_instagram_url(r.text)
    # if instagram_urls and len(instagram_urls) > 0:
    #     for instagram_url in instagram_urls:
    #         session = requests.Session()
    #         i_r = session.get(instagram_url, headers=HEADERS, timeout=10)
    #         session.get(url, headers=HEADERS, timeout=10)
    #         i_r.text
    #         candidates = parse_instagram(html=i_r.text)
    #         if candidates:
    #             file_name = process_candidates(candidates, domain)
    #             if file_name:
    #                 return file_name
    if candidates:
        file_name = process_candidates(candidates, domain)
        if file_name:
            return file_name
    return None


def find_instagram_url(html: str) -> list[str]:
    instagram_urls = []
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        if "instagram.com" in link["href"]:
            instagram_urls.append(link["href"])
    return list(set(instagram_urls))


def process_candidates(candidates: list[str], domain: str) -> str | None:
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
    f_name = file_path.name
    file_format = file_path.suffix[1:]
    image_format = "image/" + file_format
    s3_client = get_s3_client("digi-cloud")
    response = s3_client.put_object(
        Bucket=CONFIG["digi-cloud"]["bucket"],
        Key=f"company-logos/{domain}/{f_name}",
        ACL="public-read",
        Body=file_path.read_bytes(),
        # ExtraArgs={"ContentType": image_format},
        ContentType=image_format,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"Uploaded {domain} logo to S3")
    else:
        logger.error(f"Failed to upload {domain} logo to S3")


def upload_and_update(
    company_id: int, domain: str, filename: str, database_connection
) -> None:
    upload_company_logo_to_s3(
        domain=domain,
        file_path=pathlib.Path(OUTPUT_DIR, domain, filename),
    )
    logger.info(f"Uploaded {domain}  to S3")
    logo_url = f"company-logos/{domain}/{filename}"
    logger.info(f"Updating {domain} logo url to {filename}")
    logo_url = f"company-logos/{domain}/{filename}"
    update_company_logo_url(
        company_id=company_id,
        logo_url=logo_url,
        database_connection=database_connection,
    )


def update_company_logos() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    use_ssh_tunnel = False
    database_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)
    companies = query_companies(database_connection=database_connection)
    companies = companies[
        (companies["company_logo_url"].isna()) | (companies["company_logo_url"] == "")
    ]
    logger.info(f"Processing {companies.shape[0]} companies")
    i = 0
    for _i, row in companies.iterrows():
        i += 1
        domain = row["company_domain"]
        official_linkedin_url = row["company_linkedin_url"]
        logger.info(f"Start i={i}/{companies.shape[0]} {domain}")
        company_id = row["company_id"]
        filename = check_logo_exists_s3(domain)
        if filename and row["company_logo_url"]:
            logger.info(f"{domain} ok")
            continue
        # if filename and not row["company_logo_url"]:
        #     upload_and_update(
        #         company_id=company_id,
        #         domain=domain,
        #         filename=filename,
        #         database_connection=database_connection,
        #     )
        try_these = ["", "/about", "/company", "/about-us", "/about-company"]
        if "github.com" in domain:
            try_these = [""]
        for try_this in try_these:
            check_domain = domain + try_this
            filename = process_site(
                domain=domain,
                check_domain=check_domain,
                official_linkedin_url=official_linkedin_url,
            )
            if filename:
                break
        if not filename:
            filename = try_guessing(domain=domain)
            if not filename:
                logger.info(f"No logo found for {domain}")
                continue
        upload_and_update(
            company_id=company_id,
            domain=domain,
            filename=filename,
            database_connection=database_connection,
        )

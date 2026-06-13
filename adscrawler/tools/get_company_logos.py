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
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup
from PIL import Image

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import (
    query_companies,
    query_company_countries_resolved,
    query_countries,
    update_company_linkedin_url,
    update_company_logo_url,
    upsert_df,
)
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


def extract_linkedin_path(url: str) -> str | None:
    """Extract the path from a LinkedIn URL.
    e.g. 'https://linkedin.com/company/appgoblin-ex-' -> 'company/appgoblin-ex-'
    """
    match = re.search(r"(?:www\.)?linkedin\.com/(.+)", url, re.I)
    if match:
        return match.group(1).rstrip("/")
    return None


FAVICON_FILENAMES = [
    "/favicon.ico",
    "/favicon.png",
    "/apple-touch-icon.png",
    "/apple-touch-icon-precomposed.png",
    "/android-chrome-192x192.png",
    "/android-chrome-512x512.png",
]


def try_guessing(domain: str) -> tuple[str | None, str | None]:
    logger.info("Try Guessing LinkedIn")
    linkedin_base_url = "https://www.linkedin.com/company/"
    company_name = tldextract.extract(domain).domain
    guessed_url = linkedin_base_url + company_name
    l_r = requests.get(guessed_url, headers=HEADERS, timeout=10)
    linkedin_candidates = parse_linkedin(html=l_r.text)
    if linkedin_candidates:
        file_name = process_candidates(linkedin_candidates, domain)
        if file_name:
            return file_name, extract_linkedin_path(guessed_url)
    return None, extract_linkedin_path(guessed_url)


def search_duckduckgo_for_linkedin(company_name: str) -> str | None:
    """Search DuckDuckGo for 'COMPANY_NAME linkedin' using Selenium Firefox
    and return the first linkedin.com/company/ URL found, or None."""
    query = f"{company_name} linkedin"
    logger.info(f"Searching DuckDuckGo for '{query}' (Selenium Firefox)")

    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    options = Options()
    options.add_argument("--headless")
    service = Service(executable_path="/snap/bin/geckodriver")
    driver = webdriver.Firefox(options=options, service=service)
    try:
        driver.get(f"https://html.duckduckgo.com/html/?q={query.replace(' ', '+')}")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        for a in soup.find_all("a", class_="result__a"):
            href = a.get("href", "")
            if "uddg=" in href:
                params = parse_qs(urlparse(href).query)
                decoded = params.get("uddg", [""])[0]
                if decoded:
                    href = decoded
            match = re.search(r"(?:www\.)?linkedin\.com/(company/[^/\?]+)", href, re.I)
            if match:
                path = match.group(1).rstrip("/")
                logger.info(f"DuckDuckGo found LinkedIn: {path}")
                return path

        # Fallback: scan all links
        for a in soup.find_all("a", href=True):
            match = re.search(r"(?:www\.)?linkedin\.com/(company/[^/\?]+)", a["href"], re.I)
            if match:
                path = match.group(1).rstrip("/")
                logger.info(f"DuckDuckGo found LinkedIn: {path}")
                return path

        logger.info("DuckDuckGo found no LinkedIn company page")
        return None
    except Exception as e:
        logger.error(f"DuckDuckGo search failed: {e}")
        return None
    finally:
        driver.quit()


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
            try:
                l_r = requests.get(linkedin_url, headers=HEADERS, timeout=10)
                if l_r.status_code == 200:
                    linkedin_candidates = parse_linkedin(html=l_r.text)
                    if linkedin_candidates:
                        candidates += linkedin_candidates
            except requests.RequestException:
                logger.warning(f"Failed to fetch {linkedin_url}")
                continue
    return candidates


def process_site(
    domain: str, check_domain: str, official_linkedin_url: str | None = None
) -> tuple[str | None, str | None]:
    candidates = []
    linkedin_url = None
    if official_linkedin_url:
        linkedin_urls = [official_linkedin_url]
        candidates += crawl_linked_in_urls(linkedin_urls=linkedin_urls)
        if candidates:
            file_name = process_candidates(candidates, domain)
            if file_name:
                return file_name, extract_linkedin_path(official_linkedin_url)
    if "github.com-" in check_domain:
        check_domain = check_domain.replace("github.com-", "github.com/")
    try:
        logger.info(f"Try loading: {check_domain}")
        r = requests.get(f"https://{check_domain}", headers=HEADERS, timeout=10)
        if r.status_code != 200:
            logger.error(f"Failed to get {check_domain}")
            return None, None
    except Exception:
        logger.error(f"Failed to get {check_domain}")
        return None, None
    html = r.text
    if "github.com" in check_domain:
        logger.info("Found github.com in domain")
        candidates = parse_github(html=html)
    linkedin_urls = find_other_domains(other_tld="linkedin.com", html=html)
    # Extract the first LinkedIn URL for saving
    for lu in linkedin_urls:
        path = extract_linkedin_path(lu)
        if path:
            linkedin_url = path
            break
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
    if candidates:
        file_name = process_candidates(candidates, domain)
        if file_name:
            return file_name, linkedin_url
    return None, linkedin_url


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


def upload_and_update(company_id: int, domain: str, filename: str, pgdb) -> None:
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
        pgdb=pgdb,
    )


def _resolve_country_id(
    address_string: str,
    country_id_map: dict[str, int],
    name_to_alpha2: dict[str, str],
) -> int | None:
    """Guess alpha-2 from an address, then map to countries.id."""
    alpha2 = guess_country_local(address_string, name_to_alpha2)
    if alpha2 and alpha2 in country_id_map:
        return country_id_map[alpha2]
    return None


def _process_linkedin_country(
    company_id: int,
    linkedin_path: str | None,
    country_id_map: dict[str, int],
    name_to_alpha2: dict[str, str],
    pgdb,
) -> None:
    """Scrape LinkedIn about page and insert country evidence from HQ and locations."""
    if not linkedin_path:
        return

    about = scrape_linkedin_about(linkedin_path)
    if not about:
        return

    # Try HQ first, then locations for country evidence
    sources: list[tuple[str, str | None]] = []

    if about.get("headquarters"):
        sources.append(("headquarters", about["headquarters"]))

    if about.get("locations"):
        for loc in about["locations"]:
            sources.append(("location", loc))

    for label, raw_value in sources:
        if not raw_value:
            continue
        country_id = _resolve_country_id(raw_value, country_id_map, name_to_alpha2)
        insert_company_country_evidence(
            company_id=company_id,
            source="linkedin",
            raw_value=f"{label}: {raw_value}",
            country_id=country_id,
            pgdb=pgdb,
        )


def scrape_linkedin_about(linkedin_path: str) -> dict:
    """Scrape public LinkedIn company about page for details.

    Parameters
    ----------
    linkedin_path : str
        The path portion of the LinkedIn URL, e.g. 'company/igaworks'
        or 'company/igaworks/'.

    Returns
    -------
    dict
        A dictionary with keys:
        - 'description' (str or None): the "About us" description text
        - 'website' (str or None): the website URL
        - 'headquarters' (str or None): the headquarters location
        - 'locations' (list[str] or None): list of location address strings
    """
    path = linkedin_path.strip().rstrip("/")
    url = f"https://www.linkedin.com/{path}/"
    result: dict[str, object] = {
        "description": None,
        "website": None,
        "headquarters": None,
        "locations": None,
    }

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logger.error(f"LinkedIn page returned {r.status_code} for {url}")
            return result
    except requests.RequestException as e:
        logger.error(f"Failed to fetch LinkedIn page {url}: {e}")
        return result

    soup = BeautifulSoup(r.text, "html.parser")

    # --- About us / description ---
    desc_el = soup.find(attrs={"data-test-id": "about-us__description"})
    if desc_el:
        result["description"] = desc_el.get_text(strip=True)

    # --- Details from the <dl> inside About us ---
    # Website
    website_el = soup.find(attrs={"data-test-id": "about-us__website"})
    if website_el:
        link = website_el.find("a")
        if link:
            result["website"] = link.get_text(strip=True)
        else:
            dd = website_el.find("dd")
            if dd:
                result["website"] = dd.get_text(strip=True)

    # Headquarters
    hq_el = soup.find(attrs={"data-test-id": "about-us__headquarters"})
    if hq_el:
        dd = hq_el.find("dd")
        if dd:
            result["headquarters"] = dd.get_text(strip=True)

    # --- Locations ---
    locations: list[str] = []
    for section in soup.find_all("section"):
        h2 = section.find(["h2", "h3"])
        if h2 and h2.get_text(strip=True) == "Locations":
            address_divs = section.find_all("div", id=lambda x: x and x.startswith("address-"))
            for addr in address_divs:
                parts = [p.get_text(strip=True) for p in addr.find_all("p") if p.get_text(strip=True)]
                if parts:
                    locations.append("\n".join(parts))
            break

    if locations:
        result["locations"] = locations

    return result


def _build_name_to_alpha2(countries_df: pd.DataFrame) -> dict[str, str]:
    """Build a lowercase-name -> alpha2 lookup from all language columns."""
    lang_cols = [
        c for c in countries_df.columns
        if c.startswith("lang") and c != "langen"  # langen is the primary
    ] + ["langen"]
    name_map: dict[str, str] = {}
    for _, row in countries_df.iterrows():
        alpha2 = str(row["alpha2"]).strip().upper()
        if not alpha2:
            continue
        # Add alpha2 lookup
        name_map[alpha2] = alpha2
        # Add alpha3 lookup
        alpha3 = str(row.get("alpha3", "")).strip().upper()
        if alpha3:
            name_map[alpha3] = alpha2
        # Add all language names
        for col in lang_cols:
            val = row.get(col)
            if val and isinstance(val, str) and val.strip():
                key = val.strip().lower()
                name_map[key] = alpha2
    return name_map


def guess_country_local(
    address_string: str,
    name_to_alpha2: dict[str, str] | None = None,
) -> str | None:
    """Guess the ISO alpha-2 country code from an address string.

    Uses a pre-built name->alpha2 lookup (from _build_name_to_alpha2)
    for ISO lookups and falls back to common aliases.
    """
    if not address_string or not address_string.strip():
        return None

    normalized_address = address_string.lower()

    # 1. Handle common edge-case aliases first (catches phrases like
    #    "korea, south", "usa" which aren't single tokens)
    aliases = {
        "korea, south": "KR",
        "south korea": "KR",
        "republic of korea": "KR",
        "usa": "US",
        "united states of america": "US",
        "uk": "GB",
        "united kingdom": "GB",
    }
    for alias, alpha2 in aliases.items():
        if alias in normalized_address:
            return alpha2

    # 2. If we have a name map, check full names in all languages
    #    (matches "South Korea" via langen, "Corea del Sur" via langes, etc.)
    if name_to_alpha2 is not None:
        for name, alpha2 in sorted(
            name_to_alpha2.items(), key=lambda x: -len(x[0])
        ):
            if name in normalized_address:
                return alpha2

    # 3. Check tokens from right to left (countries are usually at the end)
    tokens = re.findall(r"\b\w+\b", address_string.upper())
    for token in reversed(tokens):
        if name_to_alpha2 is not None and token in name_to_alpha2:
            return name_to_alpha2[token]
        # Without a name map, fall back to matching any 2-letter alpha code
        if len(token) == 2 and token.isalpha() and name_to_alpha2 is None:
            return token

    return None


def _build_country_map(pgdb) -> tuple[dict[str, int], dict[str, str]]:
    """Build (alpha2 -> countries.id, name -> alpha2) mappings."""
    countries_df = query_countries(pgdb=pgdb)
    id_map: dict[str, int] = {}
    for _, row in countries_df.iterrows():
        alpha2 = str(row["alpha2"]).strip().upper()
        if alpha2:
            id_map[alpha2] = int(row["id"])
    name_map = _build_name_to_alpha2(countries_df)
    return id_map, name_map


def insert_company_country_evidence(
    company_id: int,
    source: str,
    raw_value: str,
    country_id: int | None,
    pgdb,
) -> None:
    """Upsert a row into adtech.company_country_evidence.

    Parameters
    ----------
    company_id : int
        The company's id in adtech.companies.
    source : str
        One of: 'linkedin', 'github', 'clearbit', 'scraping', 'app_store', 'domain_tld'.
    raw_value : str
        The raw address/evidence string.
    country_id : int | None
        The resolved country id (from countries table), or None if unknown.
    pgdb : PostgresEngine
    """
    df = pd.DataFrame(
        [
            {
                "company_id": company_id,
                "source": source,
                "raw_value": raw_value,
                "country_id": country_id,
            }
        ]
    )
    upsert_df(
        df=df,
        table_name="company_country_evidence",
        pgdb=pgdb,
        key_columns=["company_id", "source"],
        insert_columns=["company_id", "source", "raw_value", "country_id"],
        on_conflict_update=True,
        schema="adtech",
    )

def _process_single_company(
    row: pd.Series,
    country_id_map: dict[str, int],
    name_to_alpha2: dict[str, str],
    pgdb,
    *,
    needs_logo: bool,
    needs_country: bool,
) -> None:
    """Process one company row: discover logo, LinkedIn URL, and/or country evidence.

    Parameters
    ----------
    needs_logo : bool
        Whether the company is missing a logo and should be scraped.
    needs_country : bool
        Whether the company is missing a resolved country and should have its
        LinkedIn about page scraped.
    """
    # If nothing needed, skip
    if not needs_logo and not needs_country:
        return

    domain = row["company_domain"]
    existing_linkedin_url = row["company_linkedin_url"]
    existing_linkedin_url = existing_linkedin_url if type(existing_linkedin_url) == str else None
    company_id = row["company_id"]
    logger.info(f"Processing {domain} (id={company_id}) needs_logo={needs_logo} needs_country={needs_country}")

    # --- Pass 0: If we have no LinkedIn URL, try DuckDuckGo once as a fallback ---
    linkedin_url = existing_linkedin_url
    if not linkedin_url and (needs_logo or needs_country):
        company_name = row.get("company_name")
        if company_name and isinstance(company_name, str) and company_name.strip():
            ddg_url = search_duckduckgo_for_linkedin(company_name)
            if ddg_url:
                logger.info(f"Saving linkedin url for {domain}: {ddg_url}")
                update_company_linkedin_url(
                    company_id=company_id,
                    linkedin_url=ddg_url,
                    pgdb=pgdb,
                )
                linkedin_url = ddg_url

    # --- Pass 1: Logo discovery (also may discover a LinkedIn URL) ---
    if needs_logo:
        official_linkedin_url = None
        if existing_linkedin_url:
            official_linkedin_url = 'linkedin.com/' + existing_linkedin_url

        filename = check_logo_exists_s3(domain)
        if filename and row["company_logo_url"]:
            logger.info(f"{domain} logo already set")
        else:
            found_url: str | None = None
            try_these = ["", "/about", "/company", "/about-us", "/about-company"]
            if "github.com" in domain:
                try_these = [""]
            for try_this in try_these:
                check_domain = domain + try_this
                filename, found_url = process_site(
                    domain=domain,
                    check_domain=check_domain,
                    official_linkedin_url=official_linkedin_url,
                )
                if filename:
                    break
            if not filename:
                guessed_linkedin_url: str | None = None
                filename, guessed_linkedin_url = try_guessing(domain=domain)
                if not found_url:
                    found_url = guessed_linkedin_url
            # Update LinkedIn URL in DB if we discovered a new one
            if found_url and not existing_linkedin_url:
                logger.info(f"Saving linkedin url for {domain}: {found_url}")
                update_company_linkedin_url(
                    company_id=company_id,
                    linkedin_url=found_url,
                    pgdb=pgdb,
                )
                linkedin_url = found_url
            # Upload logo if found
            if filename:
                upload_and_update(
                    company_id=company_id,
                    domain=domain,
                    filename=filename,
                    pgdb=pgdb,
                )
            else:
                logger.info(f"No logo found for {domain}")
    else:
        logger.info(f"{domain} logo not needed, skipping")

    # --- Pass 2: Country evidence from LinkedIn about page ---
    if needs_country:
        if linkedin_url:
            _process_linkedin_country(
                company_id=company_id,
                linkedin_path=linkedin_url,
                country_id_map=country_id_map,
                name_to_alpha2=name_to_alpha2,
                pgdb=pgdb,
            )
        else:
            logger.info(f"{domain} no linkedin url available for country detection")
    else:
        logger.info(f"{domain} country not needed, skipping")


def process_new_company(company_name: str) -> None:
    """Process a single company just created via insert_new.py.

    Discovers its logo, LinkedIn URL, and infers country from the LinkedIn about page.
    """
    pgdb = get_db_connection()
    companies = query_companies(pgdb=pgdb)
    match = companies[companies["company_name"] == company_name]
    if match.empty:
        logger.error(f"Company '{company_name}' not found in DB")
        return

    country_id_map, name_to_alpha2 = _build_country_map(pgdb)
    _process_single_company(
        row=match.iloc[0],
        country_id_map=country_id_map,
        name_to_alpha2=name_to_alpha2,
        pgdb=pgdb,
        needs_logo=True,
        needs_country=True,
    )


def update_company_logos() -> None:
    """Legacy wrapper — processes companies missing a logo.
    Use refresh_missing_logos_and_countries() for broader coverage."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pgdb = get_db_connection()
    country_id_map, name_to_alpha2 = _build_country_map(pgdb)

    companies = query_companies(pgdb=pgdb)
    companies = companies[
        (companies["company_logo_url"].isna()) | (companies["company_logo_url"] == "")
    ]
    logger.info(f"Processing {companies.shape[0]:,} companies (missing logo)")
    resolved = query_company_countries_resolved(pgdb)
    companies = companies.merge(resolved, on="company_id", how="left")
    i = 0
    for _i, row in companies.iterrows():
        i += 1
        logger.info(f"--- i={i}/{companies.shape[0]:,} ---")
        _process_single_company(
            row=row,
            country_id_map=country_id_map,
            name_to_alpha2=name_to_alpha2,
            pgdb=pgdb,
            needs_logo=True,
            needs_country=pd.isna(row.get("country")),
        )


def refresh_missing_logos_and_countries() -> None:
    """Periodic batch function that finds companies missing a logo OR missing
    a resolved country, and processes whichever is needed per company.

    A company is fully skipped only when it already has both a logo AND a
    resolved country in company_country_evidence.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pgdb = get_db_connection()
    country_id_map, name_to_alpha2 = _build_country_map(pgdb)

    companies = query_companies(pgdb=pgdb)
    resolved = query_company_countries_resolved(pgdb)
    companies = companies.merge(resolved, on="company_id", how="left")

    has_logo = companies["company_logo_url"].notna() & (companies["company_logo_url"] != "")
    has_country = companies["country"].notna()
    companies["_needs_logo"] = ~has_logo
    companies["_needs_country"] = ~has_country

    needed = companies[companies["_needs_logo"] | companies["_needs_country"]]
    logger.info(
        f"Processing {needed.shape[0]:,} companies "
        f"(logo needed: {needed['_needs_logo'].sum()}, "
        f"country needed: {needed['_needs_country'].sum()})"
    )
    i = 0
    for _i, row in needed.iterrows():
        i += 1
        logger.info(f"--- i={i}/{needed.shape[0]:,} ---")
        _process_single_company(
            row=row,
            country_id_map=country_id_map,
            name_to_alpha2=name_to_alpha2,
            pgdb=pgdb,
            needs_logo=row["_needs_logo"],
            needs_country=row["_needs_country"],
        )


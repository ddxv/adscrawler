# Crawl App Store Apps, App SDKs & App-Ads.txt

Various tools used for collecting data for [AppGoblin's free ASO app marketing tools and app SDK lists](https://appgoblin.info). These tools are used to build AppGoblin. This is a large monorepo combining other popular tools along with a database schema to store them in.

Scrapers:

- pull apps from app store and google play store top lists [digitalmethodsinitiative/itunes-app-scraper](https://github.com/digitalmethodsinitiative/itunes-app-scraper) & [facundoolano/google-play-scraper](https://github.com/facundoolano/google-play-scraper)
- pull apps from some 3rd party stores
- unzip/decompile Android APKs and iOS IPAs to look for 3rd party tracking/advertising tools, requires _manual_ setup of [iBotPeaches/apktool](https://github.com/iBotPeaches/Apktool) and [majd/ipatool](https://github.com/majd/ipatool/)
- App-ads.txt files are crawled based on the Interactive Advertising Bureau's Tech Lab specs. https://iabtechlab.com/ads-txt/
- Implementation of [Waydroid emulator with MITM to capture HTTPs API traffic](https://github.com/ddxv/mobile-network-traffic) (requires _manual_ steps for the headless implementation, please reach out if you need help)

## Setup and Installation

- PostgreSQL: 17/18
- Setup your database. The files here use the database name 'madrone'
- Add a password to your default db user if you dont have one yet `ALTER USER postgres WITH PASSWORD 'xxx';`

- Python environment: Python 3.13
- Setup python environment `python3.12 -m venv .virtualenv` & `source .virtualenv/bin/activate`
- `uv pip install -r pyproject.toml`
- `cp example_config.toml ~/config/adscrawler/config.toml` and edit any needed values. For using all locally, the main thing that needs to be modified is the `xxx` for postgres pass and S3 host.
- In your virtualenv, init db `python db_init.py` -> Initializes MVs, inserts 3m+ apps' store_ids from https://github.com/ddxv/appgoblin-data

- Google Play App Ranks Require: NodeJS
- `npm install --save google-play-scraper`

- an S3 bucket used by app ranks, APK/IPA download, MITM

## Run

- From your environment run `python main.py` to check setup. This will also help verify the database connection is working.

- `-l, --limit-processes` Ensure only one instance of adscrawler runs. If another instance is detected, the script will not run. This includes some some options like `-p apple`

- `-t, --use-ssh-tunnel` Include to use SSH port forwarding to connect to a remote PostgreSQL database based on your `~/.config/adscrawler/config.toml`

- `-p, --platforms` Specify platforms to target. Can be `"google"`, `"apple"`, or both. Can be repeated multiple times. Default: `[]`.

### Scrape App Stores

- `-u, --update-app-store-details` Scrape app stores for app details (e.g., downloads, ratings). Requires existing store IDs.

- `--workers` Number of workers to use for updating app store details. Default: `1`.

- `-n, --new-apps-check` This crawls app rank data and stores to S3. It is also the source of new apps. Crawl the iTunes and Play Store front pages to discover new apps. Checks top apps for each category and collection.

- `--daily-s3-imports` This processes and imports data stored in S3. App Ranks and app metrics.

- `-d, --new-apps-check-devs` Crawl developers' pages to find new apps from those developers.

- `--limit-query-rows` Number of rows per run, default 200,000.

### Scrape App-Ads.txt

- `-a, --app-ads-txt-scrape` Crawl developer URLs for `app-ads.txt` files. Requires store IDs and app details to be scraped first.

### Process APKs and IPAs

- `--download-apks` Download APK files for Android or iOS apps.

- `--process-sdks` Process APKs, IPAs, and manifest files to extract SDK information.

- `-k, --crawl-keywords` Crawl keywords from app stores.

### Waydroid specific options

- `-w, --waydroid` Run apps using Waydroid.

- `-s, --store-id` Waydroid specific. Launch a specific store ID in Waydroid.

- `--timeout-waydroid` Waydroid specific. Timeout in seconds for Waydroid to run an app. Default: `180`.

- `--redownload-geo-dbs` Waydroid specific. Redownload geo databases.

- `--creative-scan-all-apps` Scan all MITM files in an S3 bucket apps for creatives.

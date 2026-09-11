"""Configuration settings for CS2 demo pipeline."""
import shutil
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
EVENTS_CACHE_DIR = CACHE_DIR / "events"
MATCHES_CACHE_DIR = CACHE_DIR / "matches"
CATALOG_DIR = DATA_DIR / "catalog"
DATABASE_PATH = CATALOG_DIR / "cs2_pro_demos.sqlite"
ARCHIVES_DIR = DATA_DIR / "archives"
DEMOS_DIR = DATA_DIR / "demos"

# Extraction tool paths (prefer unar for full RAR5 support on macOS)
UNAR_PATH = shutil.which("unar") or "/opt/homebrew/bin/unar"
SEVEN_ZIP_PATH = shutil.which("7zz") or "/opt/homebrew/bin/7zz"

# Ensure directories exist
for path in (
    DATA_DIR,
    CACHE_DIR,
    EVENTS_CACHE_DIR,
    MATCHES_CACHE_DIR,
    CATALOG_DIR,
    ARCHIVES_DIR,
    DEMOS_DIR,
):
    path.mkdir(parents=True, exist_ok=True)

# HLTV URLs
HLTV_BASE_URL = "https://www.hltv.org"
HLTV_MVP_EVENTS_URL = (
    "https://www.hltv.org/stats/events"
    "?csVersion=CS2&startDate=2023-01-01&endDate=2026-12-31&matchType=MvpEvents"
)

# Scraping & Caution Parameters
MIN_REQUEST_DELAY_SECONDS = 5.0
MAX_REQUEST_DELAY_SECONDS = 9.0
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 45.0

# Realistic Browser Headers
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

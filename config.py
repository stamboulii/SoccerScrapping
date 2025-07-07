# config.py

from pathlib import Path

# === Paths ===
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = DATA_DIR / "exports"
LOG_DIR = BASE_DIR / "logs"

# === Database ===
DATABASE_PATH = DATA_DIR / "soccer.db"

# === Scraping ===
MAX_CONCURRENT_REQUESTS = 5
SCRAPE_TIMEOUT = 10
SCRAPE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# === Logging ===
LOG_TO_CONSOLE = True
LOG_TO_FILE = True
LOG_FILE_NAME = LOG_DIR / "soccer_scraper.log"
LOG_LEVEL = "INFO"

# === Export Options ===
CSV_EXPORT_LIMIT = 10000

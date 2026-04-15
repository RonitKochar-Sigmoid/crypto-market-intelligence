import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def get_env(key: str, required: bool = True) -> str:
    """Fetch an environment variable, raise if missing and required."""
    value = os.getenv(key)
    if required and not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


def utc_now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def today_partition() -> str:
    """Return today's date string for partition paths. e.g. 2024-01-15"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def build_blob_path(source: str, endpoint: str, ingestion_date: str, filename: str) -> str:
    """
    Build a consistent ADLS blob path.
    Pattern: source=<source>/ingestion_date=<date>/<endpoint>/<filename>
    Example: source=coinmarketcap/ingestion_date=2024-01-15/listings/listings_143000.json
    """
    return f"source={source}/ingestion_date={ingestion_date}/{endpoint}/{filename}"
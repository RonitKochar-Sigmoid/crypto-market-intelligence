"""
dashboard/data_loader.py
========================
Reads Gold-layer parquet files from ADLS using the EXACT confirmed schemas:

  gold/daily_price_summary/
  ─────────────────────────
  event_timestamp              datetime (UTC)
  coin_id                      string  (CMC numeric id as string, e.g. "1")
  symbol                       string  e.g. "BTC"
  name                         string  e.g. "Bitcoin"
  price_usd                    float64
  volume_24h                   float64
  market_cap                   float64
  silver_processing_timestamp  datetime
  prev_day_price               float64
  daily_return                 float64  (fractional e.g. 0.005492 = +0.55%)
  7d_moving_avg                float64
  14d_moving_avg               float64
  7d_avg_volume                float64
  volume_spike                 int/float  (0 or 1 flag)
  total_market_cap             float64  (NaN on older rows)
  market_dominance_pct         float64  (NaN on older rows)

  gold/volatility_metrics/
  ────────────────────────
  symbol                       string
  rolling_std                  float64
  market_cap                   float64
  event_timestamp              datetime (UTC)

Connection
──────────
Reads ONLY from AZURE_STORAGE_CONNECTION_STRING in .env.
No UI input. No demo fallback.
Raises ADLSConnectionError on any auth / network failure — app.py
catches it and shows a full-page error instead of blank or demo data.
"""

from __future__ import annotations

import io
import os
import logging

import pandas as pd
from dotenv import load_dotenv

load_dotenv()          # reads .env at import time

logger = logging.getLogger(__name__)

# ── Azure SDK ──────────────────────────────────────────────────────────────────
try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# ── Constants ──────────────────────────────────────────────────────────────────
GOLD_CONTAINER = "coin-market-cap-api-data"

AVAILABLE_TABLES = {
    "daily_price_summary": "daily_price_summary",
    "volatility_metrics":  "volatility_metrics",
}

DEDUP_KEYS = {
    "daily_price_summary": ["coin_id", "event_timestamp"],
    "volatility_metrics":  ["symbol",  "event_timestamp"],
}


# ══════════════════════════════════════════════════════════════════════════════
# Custom exception
# ══════════════════════════════════════════════════════════════════════════════

class ADLSConnectionError(Exception):
    """Raised when Azure cannot be reached. Carries a human-readable message."""
    pass


# ══════════════════════════════════════════════════════════════════════════════
# Loader
# ══════════════════════════════════════════════════════════════════════════════

class ADLSDataLoader:
    """
    Reads parquet files from the Gold container.
    Raises ADLSConnectionError on any failure so the caller can show
    a clear error page instead of silently returning empty/demo data.
    """

    def __init__(self):
        if not AZURE_AVAILABLE:
            raise ADLSConnectionError(
                "azure-storage-blob is not installed.\n"
                "Run:  pip install -r requirements.txt"
            )

        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not conn_str:
            raise ADLSConnectionError(
                "AZURE_STORAGE_CONNECTION_STRING is missing.\n"
                "Add it to your .env file and restart the app."
            )

        try:
            self._client = BlobServiceClient.from_connection_string(conn_str)
            # Auth smoke-test: list containers (fast, cheap call)
            list(self._client.list_containers(results_per_page=1))
            logger.info("ADLSDataLoader: connected to Azure successfully.")
        except Exception as exc:
            raise ADLSConnectionError(
                f"Cannot connect to Azure Blob Storage.\n"
                f"Verify your AZURE_STORAGE_CONNECTION_STRING in .env\n\n"
                f"Detail: {exc}"
            ) from exc

    # ── Internal ───────────────────────────────────────────────────────────────

    def _list_parquet_blobs(self, prefix: str) -> list[str]:
        cc = self._client.get_container_client(GOLD_CONTAINER)
        return [
            b.name for b in cc.list_blobs(name_starts_with=prefix)
            if b.name.endswith(".parquet") and "_delta_log" not in b.name
        ]

    def _read_blob(self, blob_name: str) -> pd.DataFrame:
        bc  = self._client.get_blob_client(container=GOLD_CONTAINER, blob=blob_name)
        raw = bc.download_blob().readall()
        return pd.read_parquet(io.BytesIO(raw))

    # ── Public ─────────────────────────────────────────────────────────────────

    def load_table(self, table_name: str) -> pd.DataFrame:
        """
        Read all parquet files for one table.
        Returns empty DataFrame if the table folder has no files yet
        (pipeline hasn't run) — the dashboard shows a 'no data' notice.
        """
        prefix = "gold/" + AVAILABLE_TABLES.get(table_name, table_name) + "/"
        blobs  = self._list_parquet_blobs(prefix)

        if not blobs:
            logger.warning(f"gold/{prefix}: no parquet files found.")
            return pd.DataFrame()

        df = pd.concat([self._read_blob(b) for b in blobs], ignore_index=True)
        logger.info(f"{table_name}: {len(df):,} rows from {len(blobs)} file(s).")

        # Normalise timestamp column to UTC
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)

        # Deduplicate — latest version of each record wins
        keys = DEDUP_KEYS.get(table_name, [])
        if keys and all(k in df.columns for k in keys):
            df = (
                df.sort_values("event_timestamp")
                  .drop_duplicates(subset=keys, keep="last")
                  .reset_index(drop=True)
            )

        return df

    def load_all(self) -> dict[str, pd.DataFrame]:
        return {name: self.load_table(name) for name in AVAILABLE_TABLES}


# ── Public factory ─────────────────────────────────────────────────────────────

def get_loader() -> ADLSDataLoader:
    """
    Returns a connected ADLSDataLoader.
    Raises ADLSConnectionError if anything is wrong — app.py shows error page.
    """
    return ADLSDataLoader()
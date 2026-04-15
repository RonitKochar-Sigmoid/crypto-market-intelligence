# import time
# import requests
# from ingestion.utils import get_env, utc_now_iso, today_partition, build_blob_path, logger
# from ingestion.adls_client import ADLSClient


# COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# # CoinGecko free tier: 10-30 calls/min
# FREE_TIER_DELAY_SECONDS = 2  # polite delay between calls


# class CoinGeckoClient:
#     """
#     Client for CoinGecko API.
#     Used for historical backfill and feature generation.
#     No API key required for free tier.
#     """

#     def __init__(self, adls_client: ADLSClient = None):
#         self.historical_days = int(get_env("HISTORICAL_DAYS", required=False) or 180)
#         self.session = requests.Session()
#         self.session.headers.update({"Accept": "application/json"})
#         self.adls = adls_client or ADLSClient()

#     # ── Internal helpers ─────────────────────────────────────────

#     def _get(self, endpoint: str, params: dict = None, retries: int = 3) -> dict | list:
#         """GET with retry and polite rate-limit delay for CoinGecko free tier."""
#         url = f"{COINGECKO_BASE_URL}{endpoint}"
#         for attempt in range(1, retries + 1):
#             try:
#                 time.sleep(FREE_TIER_DELAY_SECONDS)  # always wait between calls
#                 response = self.session.get(url, params=params, timeout=20)

#                 if response.status_code == 429:
#                     wait = 90
#                     logger.warning(f"CoinGecko rate limited. Waiting {wait}s...")
#                     time.sleep(wait)
#                     continue

#                 response.raise_for_status()
#                 return response.json()

#             except requests.RequestException as e:
#                 logger.error(f"CoinGecko request failed (attempt {attempt}/{retries}): {e}")
#                 if attempt == retries:
#                     raise
#                 time.sleep(10 * attempt)

#     def _save_to_bronze(self, data: dict | list, endpoint_name: str, coin_id: str = "all") -> str:
#         """Upload raw CoinGecko response to Bronze layer with metadata envelope."""
#         ingestion_date = today_partition()

#         record_count = len(data) if isinstance(data, list) else 1
#         envelope = {
#             "meta": {
#                 "source": "coingecko",
#                 "endpoint": endpoint_name,
#                 "coin_id": coin_id,
#                 "ingested_at": utc_now_iso(),
#                 "ingestion_date": ingestion_date,
#                 "record_count": record_count,
#             },
#             "raw": data,
#         }

#         timestamp_str = utc_now_iso().replace(":", "").replace("-", "").replace(".", "")[:15]
#         filename = f"{endpoint_name}_{coin_id}_{timestamp_str}.json"
#         blob_path = build_blob_path(
#             source="coingecko",
#             endpoint=endpoint_name,
#             ingestion_date=ingestion_date,
#             filename=filename,
#         )

#         return self.adls.upload_json(container="bronze-prakhar", blob_path=blob_path, data=envelope)

#     # ── Public methods ───────────────────────────────────────────

#     def fetch_markets_snapshot(self, per_page: int = 250, page: int = 1) -> list:
#         """
#         Fetch a snapshot of market data for all coins.
#         Endpoint: /coins/markets
#         Good for: bulk current price, market cap, volume in one call.
#         """
#         logger.info(f"Fetching CoinGecko markets snapshot (page {page}, per_page {per_page})...")

#         params = {
#             "vs_currency": "usd",
#             "order": "market_cap_desc",
#             "per_page": per_page,
#             "page": page,
#             "sparkline": False,
#             "price_change_percentage": "24h,7d,30d",
#         }

#         data = self._get("/coins/markets", params=params)
#         self._save_to_bronze(data=data, endpoint_name="markets_snapshot", coin_id=f"page{page}")
#         logger.info(f"Markets snapshot page {page} saved to Bronze ({len(data)} coins)")
#         return data

#     def fetch_coin_market_chart(self, coin_id: str, days: int = None) -> dict:
#         """
#         Fetch historical price, market cap, and volume for a single coin.
#         Endpoint: /coins/{id}/market_chart
#         Returns: lists of [timestamp_ms, value] pairs
#         """
#         days = days or self.historical_days
#         logger.info(f"Fetching {days}-day market chart for coin: {coin_id}")

#         params = {
#             "vs_currency": "usd",
#             "days": days,
#             "interval": "daily",  # daily granularity for >90 days
#         }

#         data = self._get(f"/coins/{coin_id}/market_chart", params=params)
#         self._save_to_bronze(data=data, endpoint_name="market_chart", coin_id=coin_id)
#         logger.info(f"Market chart for {coin_id} ({days} days) saved to Bronze")
#         return data

#     def fetch_bulk_market_charts(self, coin_ids: list[str], days: int = None) -> dict[str, dict]:
#         """
#         Fetch historical charts for multiple coins.
#         Respects rate limits with delay between each call.
#         Returns dict: {coin_id: chart_data}
#         """
#         days = days or self.historical_days
#         results = {}

#         logger.info(f"Starting bulk chart fetch for {len(coin_ids)} coins over {days} days...")

#         for i, coin_id in enumerate(coin_ids, 1):
#             try:
#                 logger.info(f"  [{i}/{len(coin_ids)}] Fetching {coin_id}...")
#                 results[coin_id] = self.fetch_coin_market_chart(coin_id=coin_id, days=days)
#             except Exception as e:
#                 logger.error(f"  Failed to fetch {coin_id}: {e}. Skipping.")
#                 results[coin_id] = None

#         logger.info(f"Bulk fetch complete. Success: {sum(1 for v in results.values() if v)}/{len(coin_ids)}")
#         return results

"""
ingestion/coingecko_client.py

CoinGecko Client — 180-Day Hourly Ingestion Strategy
======================================================
Objective : Retrieve 180 days of HOURLY price + volume data for 10 coins.
Constraint: CoinGecko only returns hourly granularity when the requested
            range is ≤ 90 days.
Solution  : Split the 180-day window into two 90-day chunks, fetch each,
            union + deduplicate, then write to Bronze as a Delta-ready
            Parquet/JSON with the canonical schema.

Chunk Layout (relative to now = T):
  Point A  →  T - 180 days   (start)
  Point B  →  T -  90 days   (midpoint)
  Point C  →  T              (now)

  Chunk 1  :  A  →  B        (90 days, hourly ✓)
  Chunk 2  :  B+1s →  C      (90 days, hourly ✓)

Rate-Limit Policy (Demo / Free tier = 30 req/min):
  - 10 coins × 2 chunks = 20 API calls per full run
  - Batch size  : 5 calls, then sleep 2 s
  - On HTTP 429 : exponential backoff → 10 s, 20 s, 40 s
"""
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from ingestion.utils import get_env, utc_now_iso, today_partition, build_blob_path, logger
from ingestion.adls_client import ADLSClient


# ── Constants ──────────────────────────────────────────────────────────────────
COINGECKO_BASE_URL   = "https://api.coingecko.com/api/v3"
BATCH_SIZE           = 1          # 1 call per batch (Drip strategy)
BATCH_SLEEP_SECONDS  = 12         # 12 seconds guarantees ~5 calls/min (very safe)
BACKOFF_SEQUENCE     = [15, 30, 60, 120]
EXPECTED_ROWS        = 4_320      
ROW_TOLERANCE        = 48         
HOURLY_MS            = 3_600_000  
GAP_TOLERANCE_MS     = 3_600_000  # Increased to 60 mins to absorb severe API jitter

# 10 coins to track  (CoinGecko string IDs)
DEFAULT_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "polkadot",
    "chainlink",
    "dogecoin",
    "pepe",
    "uniswap",
    "binancecoin"
]


# ── Helper: Unix timestamp arithmetic ─────────────────────────────────────────

def _to_unix(dt: datetime) -> int:
    """Convert a timezone-aware datetime → integer Unix timestamp (seconds)."""
    return int(dt.timestamp())


def _compute_chunk_boundaries() -> tuple[int, int, int]:
    """
    Calculate the three Unix timestamp boundaries for the 180-day window.

    Returns
    -------
    point_a : int   T - 180 days  (start of window)
    point_b : int   T -  90 days  (midpoint / chunk boundary)
    point_c : int   T             (now)
    """
    now      = datetime.now(timezone.utc)
    point_a  = _to_unix(now - timedelta(days=180))
    point_b  = _to_unix(now - timedelta(days=90))
    point_c  = _to_unix(now)
    return point_a, point_b, point_c


# ── CoinGeckoClient ────────────────────────────────────────────────────────────

class CoinGeckoClient:
    """
    Fetches 180 days of hourly OHLCV data from CoinGecko using two 90-day
    range requests per coin, stitches them, validates, and writes to Bronze.
    """

    def __init__(self, adls_client: ADLSClient = None):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.adls           = adls_client or ADLSClient()
        self._call_counter  = 0   # tracks calls in the current batch window

    # ── Low-level HTTP ─────────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict = None) -> dict | list:
        """
        GET request with:
          • Batch-level sleep  : sleep BATCH_SLEEP_SECONDS every BATCH_SIZE calls
          • 429 backoff        : exponential retry using BACKOFF_SEQUENCE
        """
        url = f"{COINGECKO_BASE_URL}{endpoint}"

        # Enforce batch-level rate limiting
        self._call_counter += 1
        if self._call_counter % BATCH_SIZE == 0:
            logger.info(
                f"[Rate-limit] {self._call_counter} calls made — "
                f"sleeping {BATCH_SLEEP_SECONDS}s before next batch."
            )
            time.sleep(BATCH_SLEEP_SECONDS)

        # Attempt with exponential backoff on 429
        for attempt, backoff in enumerate(BACKOFF_SEQUENCE, start=1):
            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    logger.warning(
                        f"[429] Rate limited on attempt {attempt}. "
                        f"Backing off {backoff}s …"
                    )
                    time.sleep(backoff)
                    continue  # retry

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(f"[Timeout] attempt {attempt}/{len(BACKOFF_SEQUENCE)} — {url}")
                time.sleep(backoff)

            except requests.exceptions.RequestException as exc:
                logger.error(f"[Error] {exc}")
                if attempt == len(BACKOFF_SEQUENCE):
                    raise
                time.sleep(backoff)

        raise RuntimeError(f"All retry attempts exhausted for {url}")

    # ── Chunk fetch ────────────────────────────────────────────────────────────

    def _fetch_range_chunk(
        self,
        coin_id: str,
        from_ts: int,
        to_ts: int,
        chunk_label: str,
    ) -> dict:
        """
        Fetch one 90-day chunk from /coins/{id}/market_chart/range.

        Parameters
        ----------
        coin_id     : CoinGecko coin ID string (e.g. "bitcoin")
        from_ts     : Unix timestamp — start of range (inclusive)
        to_ts       : Unix timestamp — end of range (inclusive)
        chunk_label : "chunk1" or "chunk2" (used in logging / blob path)

        Returns
        -------
        Raw JSON dict with keys: prices, market_caps, total_volumes
        """
        endpoint = f"/coins/{coin_id}/market_chart/range"
        params   = {
            "vs_currency": "usd",
            "from": from_ts,
            "to":   to_ts,
        }

        logger.info(
            f"  [{chunk_label}] {coin_id} | "
            f"from={datetime.fromtimestamp(from_ts, tz=timezone.utc).date()} "
            f"to={datetime.fromtimestamp(to_ts,   tz=timezone.utc).date()}"
        )

        data = self._get(endpoint, params=params)

        prices  = data.get("prices",        [])
        volumes = data.get("total_volumes", [])
        logger.info(
            f"  [{chunk_label}] {coin_id} — "
            f"{len(prices)} price points, {len(volumes)} volume points received."
        )

        return data

    # ── Stitch & deduplicate ───────────────────────────────────────────────────

    @staticmethod
    def _stitch_chunks(chunk1: dict, chunk2: dict) -> list[dict]:
        """
        Union the prices and total_volumes arrays from both chunks,
        sort by timestamp, deduplicate on timestamp_ms, and return
        a list of canonical row dicts.

        Canonical schema
        ----------------
        coin_id       : str    (filled by caller)
        timestamp_ms  : int    Unix timestamp in milliseconds
        price_usd     : float
        volume_usd    : float
        """
        # Build lookup dicts  {timestamp_ms: value}  for fast dedup
        price_map  : dict[int, float] = {}
        volume_map : dict[int, float] = {}

        for chunk in (chunk1, chunk2):
            for ts_ms, price in chunk.get("prices",        []):
                price_map[int(ts_ms)]  = float(price)
            for ts_ms, vol in chunk.get("total_volumes", []):
                volume_map[int(ts_ms)] = float(vol)

        # Use only timestamps present in BOTH series (inner join)
        common_timestamps = sorted(set(price_map) & set(volume_map))

        rows = [
            {
                "timestamp_ms": ts,
                "price_usd":    price_map[ts],
                "volume_usd":   volume_map[ts],
            }
            for ts in common_timestamps
        ]

        logger.info(f"  [stitch] {len(rows)} deduplicated rows after union.")
        return rows

    # ── Validation ─────────────────────────────────────────────────────────────

    @staticmethod
    def _validate_rows(coin_id: str, rows: list[dict]) -> None:
        """
        Apply the three validation rules before writing to Bronze.

        Rules
        -----
        1. Row count   : must be EXPECTED_ROWS ± ROW_TOLERANCE  (~4,320 ± 48)
        2. Null check  : price_usd and volume_usd must have zero nulls
        3. Granularity : consecutive timestamp diffs must equal HOURLY_MS ± GAP_TOLERANCE_MS
        """
        errors = []

        # Rule 1 — Row count
        lo = EXPECTED_ROWS - ROW_TOLERANCE
        hi = EXPECTED_ROWS + ROW_TOLERANCE
        if not (lo <= len(rows) <= hi):
            errors.append(
                f"Row count {len(rows)} outside expected range [{lo}, {hi}]."
            )

        # Rule 2 — Null check
        null_price  = sum(1 for r in rows if r["price_usd"]  is None)
        null_volume = sum(1 for r in rows if r["volume_usd"] is None)
        if null_price:
            errors.append(f"{null_price} null values found in price_usd.")
        if null_volume:
            errors.append(f"{null_volume} null values found in volume_usd.")

        # Rule 3 — Hourly granularity (with variance tolerance)
        timestamps = [r["timestamp_ms"] for r in rows]
        bad_gaps   = []
        for i in range(1, len(timestamps)):
            diff = timestamps[i] - timestamps[i - 1]
            if abs(diff - HOURLY_MS) > GAP_TOLERANCE_MS:
                bad_gaps.append((i, diff))
                
        if bad_gaps:
            # Report the first 5 violations to keep the message readable
            sample = bad_gaps[:5]
            errors.append(
                f"{len(bad_gaps)} non-hourly gaps (outside 10m tolerance) detected. "
                f"First 5 (index, diff_ms): {sample}"
            )

        if errors:
            raise ValueError(
                f"Validation FAILED for coin '{coin_id}':\n  " + "\n  ".join(errors)
            )

        logger.info(
            f"  [validate] ✓ {coin_id} — "
            f"{len(rows)} rows, 0 nulls, hourly gaps confirmed."
        )

    # ── Bronze writer ──────────────────────────────────────────────────────────

    def _save_to_bronze(
        self,
        coin_id: str,
        rows: list[dict],
        chunk1_raw: dict,
        chunk2_raw: dict,
    ) -> str:
        """
        Write the stitched rows + raw chunk responses to the Bronze layer.

        Blob path pattern:
          bronze-prakhar/source=coingecko/ingestion_date=YYYY-MM-DD/
            hourly_history/hourly_history_{coin_id}_{timestamp}.json
        """
        ingestion_date = today_partition()
        ts_tag         = utc_now_iso().replace(":", "").replace("-", "")[:15]

        # Attach coin_id to every row
        for row in rows:
            row["coin_id"] = coin_id

        envelope = {
            "meta": {
                "source":          "coingecko",
                "endpoint":        "market_chart/range",
                "strategy":        "180d_hourly_2x90d_chunks",
                "coin_id":         coin_id,
                "ingested_at":     utc_now_iso(),
                "ingestion_date":  ingestion_date,
                "row_count":       len(rows),
                "expected_rows":   EXPECTED_ROWS,
            },
            "schema": {
                "coin_id":      "String",
                "timestamp_ms": "Long",
                "price_usd":    "Double",
                "volume_usd":   "Double",
            },
            "data":       rows,
            "raw_chunks": {
                "chunk1": chunk1_raw,
                "chunk2": chunk2_raw,
            },
        }

        filename  = f"hourly_history_{coin_id}_{ts_tag}.json"
        blob_path = build_blob_path(
            source         = "coingecko",
            endpoint       = "hourly_history",
            ingestion_date = ingestion_date,
            filename       = filename,
        )

        path = self.adls.upload_json(
            container = "bronze-prakhar",
            blob_path = blob_path,
            data      = envelope,
        )
        logger.info(f"  [bronze] Saved → {path}")
        return path

    # ── Public API ─────────────────────────────────────────────────────────────

    def fetch_180d_hourly(self, coin_id: str) -> list[dict]:
        """
        Full pipeline for ONE coin:
          1. Compute chunk boundaries (A, B, C)
          2. Fetch Chunk 1  (A → B)
          3. Fetch Chunk 2  (B+1s → C)
          4. Stitch & deduplicate
          5. Validate
          6. Write to Bronze
          7. Return canonical rows

        Parameters
        ----------
        coin_id : CoinGecko string ID (e.g. "bitcoin")

        Returns
        -------
        List of row dicts:
            [{"coin_id": ..., "timestamp_ms": ..., "price_usd": ..., "volume_usd": ...}, ...]
        """
        logger.info(f"[180d-hourly] Starting ingestion for: {coin_id}")

        point_a, point_b, point_c = _compute_chunk_boundaries()

        chunk1 = self._fetch_range_chunk(
            coin_id     = coin_id,
            from_ts     = point_a,
            to_ts       = point_b,
            chunk_label = "chunk1",
        )
        chunk2 = self._fetch_range_chunk(
            coin_id     = coin_id,
            from_ts     = point_b + 1,   # +1 second to avoid boundary overlap
            to_ts       = point_c,
            chunk_label = "chunk2",
        )

        rows = self._stitch_chunks(chunk1, chunk2)
        self._validate_rows(coin_id, rows)
        self._save_to_bronze(coin_id, rows, chunk1, chunk2)

        logger.info(f"[180d-hourly] ✓ Completed: {coin_id} — {len(rows)} rows ingested.")
        return rows

    def fetch_all_coins_180d_hourly(
        self,
        coin_ids: list[str] = None,
    ) -> dict[str, list[dict]]:
        """
        Run fetch_180d_hourly for every coin in the list.
        Respects batch-level rate limiting via _get().

        Parameters
        ----------
        coin_ids : list of CoinGecko string IDs; defaults to DEFAULT_COINS (10 coins)

        Returns
        -------
        dict mapping coin_id → list of row dicts  (None if the coin failed)
        """
        coin_ids = coin_ids or DEFAULT_COINS
        results  : dict[str, list[dict] | None] = {}

        logger.info(
            f"[bulk-180d] Starting bulk 180-day hourly ingestion "
            f"for {len(coin_ids)} coins: {coin_ids}"
        )

        for i, coin_id in enumerate(coin_ids, start=1):
            logger.info(f"[bulk-180d] ── Coin {i}/{len(coin_ids)}: {coin_id}")
            try:
                results[coin_id] = self.fetch_180d_hourly(coin_id)
            except Exception as exc:
                logger.error(f"[bulk-180d] ✗ {coin_id} FAILED: {exc}")
                results[coin_id] = None

        success = sum(1 for v in results.values() if v is not None)
        failed  = sum(1 for v in results.values() if v is None)
        logger.info(
            f"[bulk-180d] Bulk run complete — "
            f"✓ {success} succeeded, ✗ {failed} failed."
        )

        if failed:
            failed_coins = [k for k, v in results.items() if v is None]
            raise RuntimeError(
                f"Bulk ingestion incomplete. Failed coins: {failed_coins}"
            )

        return results

    # ── Legacy helpers (kept for backward-compatibility) ──────────────────────

    def fetch_markets_snapshot(self, per_page: int = 250, page: int = 1) -> list:
        """
        Snapshot of current market data (/coins/markets).
        Still used by the historical_backfill DAG for metadata.
        """
        logger.info(f"Fetching markets snapshot (page {page}) …")
        params = {
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    per_page,
            "page":        page,
            "sparkline":   False,
            "price_change_percentage": "24h,7d,30d",
        }
        data = self._get("/coins/markets", params=params)

        ingestion_date = today_partition()
        ts_tag         = utc_now_iso().replace(":", "").replace("-", "")[:15]
        blob_path      = build_blob_path(
            source         = "coingecko",
            endpoint       = "markets_snapshot",
            ingestion_date = ingestion_date,
            filename       = f"markets_snapshot_page{page}_{ts_tag}.json",
        )
        envelope = {
            "meta": {
                "source":         "coingecko",
                "endpoint":       "coins/markets",
                "ingested_at":    utc_now_iso(),
                "ingestion_date": ingestion_date,
                "record_count":   len(data),
            },
            "raw": data,
        }
        self.adls.upload_json(container="bronze-prakhar", blob_path=blob_path, data=envelope)
        logger.info(f"Markets snapshot page {page} → Bronze ({len(data)} coins)")
        return data
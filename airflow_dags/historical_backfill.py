# """
# DAG: historical_backfill
# Schedule: On demand (manual trigger only)
# Purpose: Pull 30-180 days of historical data from CoinGecko for model training.

# This DAG is triggered manually:
#   - When setting up the platform for the first time
#   - When adding new coins to track
#   - When reprocessing is needed

# It uses CoinGecko (no credit limits) to avoid burning CMC credits.
# """

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

# default_args = {
#     "owner": "crypto-platform",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "retries": 3,
#     "retry_delay": timedelta(minutes=5),
# }

# # CoinGecko uses string IDs (not numeric like CMC)
# # These map to the top coins we track
# COINS_TO_BACKFILL = [
#     "bitcoin", "ethereum", "binancecoin", "solana", "ripple",
#     "dogecoin", "cardano", "avalanche-2", "polkadot", "matic-network",
#     "chainlink", "uniswap", "litecoin", "stellar", "cosmos",
# ]


# # ── Task functions ─────────────────────────────────────────────────────────────

# def task_fetch_markets_snapshot(**context):
#     """
#     Fetch current snapshot of all tracked coins from CoinGecko markets endpoint.
#     Captures 24h/7d/30d price change data in one call.
#     """
#     from ingestion.coingecko_client import CoinGeckoClient
#     client = CoinGeckoClient()

#     all_coins = []
#     for page in range(1, 3):  # fetch 2 pages = 500 coins
#         page_data = client.fetch_markets_snapshot(per_page=250, page=page)
#         all_coins.extend(page_data)

#     context["ti"].xcom_push(key="total_coins_snapshot", value=len(all_coins))
#     return f"Snapshot fetched for {len(all_coins)} coins"


# def task_backfill_historical_charts(**context):
#     """
#     Fetch historical price/volume/market_cap for each coin in COINS_TO_BACKFILL.
#     This is the primary data for ML model training.
#     """
#     from ingestion.coingecko_client import CoinGeckoClient
#     import os

#     client = CoinGeckoClient()
#     days = int(os.getenv("HISTORICAL_DAYS", 180))

#     results = client.fetch_bulk_market_charts(
#         coin_ids=COINS_TO_BACKFILL,
#         days=days,
#     )

#     success = sum(1 for v in results.values() if v is not None)
#     failed  = sum(1 for v in results.values() if v is None)

#     context["ti"].xcom_push(key="backfill_success_count", value=success)
#     context["ti"].xcom_push(key="backfill_failed_count", value=failed)

#     if failed > 0:
#         raise ValueError(f"Backfill incomplete: {failed} coins failed. Check logs.")

#     return f"Backfill complete: {success}/{len(COINS_TO_BACKFILL)} coins"


# def task_validate_backfill(**context):
#     """
#     Simple validation: check that expected blobs exist in Bronze.
#     Raises an error if any coin's data is missing.
#     """
#     from ingestion.adls_client import ADLSClient
#     from ingestion.utils import today_partition

#     adls = ADLSClient()
#     ingestion_date = today_partition()
#     prefix = f"source=coingecko/ingestion_date={ingestion_date}/market_chart/"

#     blobs = adls.list_blobs(container="bronze-prakhar", prefix=prefix)
#     fetched_coins = set()
#     for blob in blobs:
#         for coin in COINS_TO_BACKFILL:
#             if coin in blob:
#                 fetched_coins.add(coin)

#     missing = set(COINS_TO_BACKFILL) - fetched_coins
#     if missing:
#         raise ValueError(f"Validation failed. Missing blobs for: {missing}")

#     return f"Validation passed. All {len(COINS_TO_BACKFILL)} coins present in Bronze."


# # ── DAG definition ─────────────────────────────────────────────────────────────

# with DAG(
#     dag_id="historical_backfill",
#     description="Backfill historical crypto data from CoinGecko (run on demand)",
#     default_args=default_args,
#     schedule_interval=None,     # Manual trigger only
#     start_date=days_ago(1),
#     catchup=False,
#     max_active_runs=1,
#     tags=["ingestion", "coingecko", "backfill", "historical"],
# ) as dag:

#     fetch_snapshot = PythonOperator(
#         task_id="fetch_markets_snapshot",
#         python_callable=task_fetch_markets_snapshot,
#     )

#     backfill_charts = PythonOperator(
#         task_id="backfill_historical_charts",
#         python_callable=task_backfill_historical_charts,
#         execution_timeout=timedelta(hours=2),  # bulk fetch can take time
#     )

#     validate = PythonOperator(
#         task_id="validate_backfill",
#         python_callable=task_validate_backfill,
#     )

#     # Run snapshot first, then backfill all charts, then validate
#     fetch_snapshot >> backfill_charts >> validate

"""
airflow_dags/historical_backfill.py

DAG: historical_backfill
========================
Schedule  : On demand (manual trigger only — schedule_interval=None)
Purpose   : Pull 180 days of HOURLY price + volume data from CoinGecko
            for 10 coins using the two-chunk 90-day range strategy.

Design
------
The DAG is intentionally lean — three sequential tasks:

  1. fetch_markets_snapshot
       → Quick current-state snapshot of all tracked coins via /coins/markets
       → Gives us 24h/7d/30d % change metadata in one cheap call
       → Pushes coin metadata to XCom for downstream visibility

  2. fetch_180d_hourly_all_coins
       → Core task: runs the full 2-chunk / stitch / validate / Bronze pipeline
         for every coin in COINS_TO_BACKFILL (10 coins = 20 API calls)
       → Rate limiting is handled inside CoinGeckoClient._get()
         (sleep 2 s every 5 calls, exponential backoff on 429)
       → Pushes per-coin success/failure summary to XCom

  3. validate_bronze_blobs
       → Confirms that the expected Bronze blobs exist in ADLS
       → Raises an error if any coin is missing, so Airflow marks the run FAILED

Trigger instructions
--------------------
  Airflow UI  :  DAGs → historical_backfill → Trigger DAG ▶
  CLI         :  airflow dags trigger historical_backfill
  API         :  POST /api/v1/dags/historical_backfill/dagRuns
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "crypto-platform",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── Coins to backfill (CoinGecko string IDs) ───────────────────────────────────
COINS_TO_BACKFILL = [
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


# ── Task 1 ─────────────────────────────────────────────────────────────────────

def task_fetch_markets_snapshot(**context):
    """
    Fetch current market snapshot from CoinGecko /coins/markets.
    Captures 24h / 7d / 30d % change data for all tracked coins.
    Pushes a summary dict to XCom.
    """
    from ingestion.coingecko_client import CoinGeckoClient

    client    = CoinGeckoClient()
    all_coins = []

    # Two pages = up to 500 coins (our 10 are definitely in page 1)
    for page in range(1, 3):
        page_data = client.fetch_markets_snapshot(per_page=250, page=page)
        all_coins.extend(page_data)

    # Build a quick-reference map for downstream tasks / logs
    snapshot_map = {
        coin["id"]: {
            "symbol":        coin.get("symbol", "").upper(),
            "current_price": coin.get("current_price"),
            "market_cap":    coin.get("market_cap"),
            "pct_24h":       coin.get("price_change_percentage_24h"),
            "pct_7d":        coin.get("price_change_percentage_7d_in_currency"),
            "pct_30d":       coin.get("price_change_percentage_30d_in_currency"),
        }
        for coin in all_coins
        if coin.get("id") in COINS_TO_BACKFILL
    }

    context["ti"].xcom_push(key="snapshot_map",   value=snapshot_map)
    context["ti"].xcom_push(key="total_snapshot",  value=len(all_coins))

    return f"Snapshot fetched for {len(all_coins)} coins; {len(snapshot_map)} tracked coins found."


# ── Task 2 ─────────────────────────────────────────────────────────────────────

def task_fetch_180d_hourly_all_coins(**context):
    """
    Core ingestion task.

    For each coin in COINS_TO_BACKFILL:
      1. Compute three Unix timestamps (T-180d, T-90d, T)
      2. Fetch Chunk 1  (T-180d  →  T-90d)   — 90 days, hourly
      3. Fetch Chunk 2  (T-90d+1s → T)       — 90 days, hourly
      4. Union + deduplicate on timestamp_ms
      5. Validate (row count ~4,320, 0 nulls, hourly gaps)
      6. Write envelope + canonical rows to Bronze

    Rate limiting is enforced inside CoinGeckoClient._get():
      • Sleep 2 s every 5 calls (20 calls total → 4 sleeps = +8 s overhead)
      • Exponential backoff on HTTP 429 (10 s → 20 s → 40 s)

    Pushes a per-coin result summary to XCom.
    """
    from ingestion.coingecko_client import CoinGeckoClient

    client  = CoinGeckoClient()
    summary = {}

    for i, coin_id in enumerate(COINS_TO_BACKFILL, start=1):
        try:
            rows = client.fetch_180d_hourly(coin_id)
            summary[coin_id] = {
                "status":    "success",
                "row_count": len(rows),
            }
        except Exception as exc:
            summary[coin_id] = {
                "status": "failed",
                "error":  str(exc),
            }

    # Push full summary to XCom for the validation task and log visibility
    context["ti"].xcom_push(key="ingestion_summary", value=summary)

    failed_coins = [k for k, v in summary.items() if v["status"] == "failed"]
    if failed_coins:
        raise RuntimeError(
            f"Hourly ingestion incomplete. Failed coins: {failed_coins}\n"
            f"Summary: {summary}"
        )

    total_rows = sum(v["row_count"] for v in summary.values())
    return (
        f"All {len(COINS_TO_BACKFILL)} coins ingested successfully. "
        f"Total rows written: {total_rows:,}"
    )


# ── Task 3 ─────────────────────────────────────────────────────────────────────

def task_validate_bronze_blobs(**context):
    """
    Confirm that exactly one Bronze blob exists for every coin.

    Checks:
      • ADLS Bronze container has a blob for each coin_id under
        source=coingecko/ingestion_date=<today>/hourly_history/
      • XCom summary from Task 2 shows 0 failures

    Raises ValueError if any coin is missing from Bronze.
    """
    from ingestion.adls_client import ADLSClient
    from ingestion.utils      import today_partition

    ti             = context["ti"]
    ingestion_date = today_partition()
    adls           = ADLSClient()
    prefix         = f"source=coingecko/ingestion_date={ingestion_date}/hourly_history/"

    # List blobs under today's hourly_history prefix
    blobs = adls.list_blobs(container="bronze-prakhar", prefix=prefix)

    # Check which coins have a blob
    confirmed_coins = set()
    for blob_name in blobs:
        for coin_id in COINS_TO_BACKFILL:
            if f"_{coin_id}_" in blob_name or blob_name.endswith(f"_{coin_id}.json"):
                confirmed_coins.add(coin_id)

    missing = set(COINS_TO_BACKFILL) - confirmed_coins

    if missing:
        raise ValueError(
            f"Bronze validation FAILED. "
            f"Missing blobs for: {sorted(missing)}. "
            f"Found blobs: {blobs}"
        )

    # Double-check against XCom summary
    summary = ti.xcom_pull(task_ids="fetch_180d_hourly_all_coins", key="ingestion_summary") or {}
    xcom_failures = [k for k, v in summary.items() if v.get("status") == "failed"]
    if xcom_failures:
        raise ValueError(
            f"XCom summary reports failures for: {xcom_failures}"
        )

    row_counts = {k: v.get("row_count") for k, v in summary.items()}
    return (
        f"Bronze validation PASSED. "
        f"All {len(COINS_TO_BACKFILL)} coins present. "
        f"Row counts: {row_counts}"
    )


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id            = "historical_backfill",
    description       = (
        "Backfill 180 days of hourly price/volume data from CoinGecko "
        "using two 90-day range chunks per coin. Manual trigger only."
    ),
    default_args      = default_args,
    schedule_interval = None,          # ← manual trigger only
    start_date        = days_ago(1),
    catchup           = False,
    max_active_runs   = 1,             # prevent parallel runs
    tags              = ["ingestion", "coingecko", "backfill", "historical", "hourly"],
) as dag:

    fetch_snapshot = PythonOperator(
        task_id          = "fetch_markets_snapshot",
        python_callable  = task_fetch_markets_snapshot,
        doc_md           = "Fetch current market snapshot from CoinGecko /coins/markets.",
    )

    fetch_hourly = PythonOperator(
        task_id           = "fetch_180d_hourly_all_coins",
        python_callable   = task_fetch_180d_hourly_all_coins,
        execution_timeout = timedelta(hours=2),  # bulk 20-call run can take time
        doc_md            = (
            "Fetch 180 days of hourly data for 10 coins "
            "using two 90-day /market_chart/range chunks per coin."
        ),
    )

    validate = PythonOperator(
        task_id         = "validate_bronze_blobs",
        python_callable = task_validate_bronze_blobs,
        doc_md          = "Verify all 10 coins have valid Bronze blobs for today.",
    )

    # ── Task dependencies (sequential) ────────────────────────────────────────
    fetch_snapshot >> fetch_hourly >> validate
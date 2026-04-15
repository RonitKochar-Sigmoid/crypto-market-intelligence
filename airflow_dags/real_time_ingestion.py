"""
DAG: real_time_ingestion
Schedule: Every 15 minutes
Purpose: Fetch latest listings and global metrics from CoinMarketCap.
         Selectively refresh quotes for top 10 key coins.

Credit budget per run:
  - listings_latest (top 100): 1 credit
  - global_metrics:            1 credit
  - quotes_latest (top 10):    1 credit
  Total: ~3 credits/run × 96 runs/day = ~288 credits/day (within 333 limit)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "crypto-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# ── Top 10 key coins (CMC IDs) for selective quote refresh ────────────────────
# BTC=1, ETH=1027, BNB=1839, SOL=5426, XRP=52, DOGE=74, ADA=2010, AVAX=5805, DOT=6636, MATIC=3890
KEY_COIN_IDS = [1, 1027, 1839, 5426, 52, 74, 2010, 5805, 6636, 3890]


# ── Task functions ─────────────────────────────────────────────────────────────

def task_fetch_listings(**context):
    import sys
    if '/opt/airflow' not in sys.path:
        sys.path.append('/opt/airflow')
    """Fetch latest listings for top N coins and save to Bronze."""
    from ingestion.coinmarketcap_client import CoinMarketCapClient
    client = CoinMarketCapClient()
    data = client.fetch_latest_listings()

    # Push coin IDs to XCom so downstream tasks can use them
    coin_ids = [coin["id"] for coin in data.get("data", [])]
    context["ti"].xcom_push(key="coin_ids", value=coin_ids[:10])  # top 10 for quotes
    return f"Fetched {len(coin_ids)} coins"


def task_fetch_global_metrics(**context):
    """Fetch global market metrics (low frequency)."""
    from ingestion.coinmarketcap_client import CoinMarketCapClient
    client = CoinMarketCapClient()
    data = client.fetch_global_metrics()
    return "Global metrics fetched"


def task_fetch_key_quotes(**context):
    """
    Selective quote refresh for key coins using IDs from XCom.
    Falls back to hardcoded KEY_COIN_IDS if XCom is empty.
    """
    from ingestion.coinmarketcap_client import CoinMarketCapClient
    ti = context["ti"]

    # Try to get coin IDs from the listings task via XCom
    coin_ids = ti.xcom_pull(task_ids="fetch_listings", key="coin_ids") or KEY_COIN_IDS

    client = CoinMarketCapClient()
    client.fetch_quotes_latest(coin_ids=coin_ids)
    return f"Quotes refreshed for {len(coin_ids)} coins"


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="real_time_ingestion",
    description="Fetch real-time crypto data from CoinMarketCap every 15 minutes",
    default_args=default_args,
    schedule_interval="*/15 * * * *",   # every 15 minutes
    start_date=days_ago(1),
    catchup=False,                       # don't backfill missed runs
    max_active_runs=1,                   # prevent overlapping runs
    tags=["ingestion", "coinmarketcap", "real-time"],
) as dag:

    fetch_listings = PythonOperator(
        task_id="fetch_listings",
        python_callable=task_fetch_listings,
    )

    fetch_global_metrics = PythonOperator(
        task_id="fetch_global_metrics",
        python_callable=task_fetch_global_metrics,
    )

    fetch_key_quotes = PythonOperator(
        task_id="fetch_key_quotes",
        python_callable=task_fetch_key_quotes,
    )

    # Dependency: fetch_listings runs first (provides coin IDs), then the other two in parallel
    fetch_listings >> [fetch_global_metrics, fetch_key_quotes]
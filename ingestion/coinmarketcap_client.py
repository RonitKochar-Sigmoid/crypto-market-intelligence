import time
import requests
from ingestion.utils import get_env, utc_now_iso, today_partition, build_blob_path, logger
from ingestion.adls_client import ADLSClient


CMC_BASE_URL = "https://pro-api.coinmarketcap.com"

# Credit cost per endpoint (approximate, from CMC docs)
CREDIT_COSTS = {
    "listings_latest": 1,    # per 200 coins
    "quotes_latest": 1,      # per 100 coins
    "global_metrics": 1,
}


class CoinMarketCapClient:
    """
    Client for CoinMarketCap Pro API.
    Handles rate limiting, credit tracking, and Bronze layer uploads.
    """

    def __init__(self, adls_client: ADLSClient = None):
        self.api_key = get_env("CMC_API_KEY")
        self.top_n   = int(get_env("TOP_N_COINS", required=False) or 100)
        self.session = requests.Session()
        self.session.headers.update({
            "X-CMC_PRO_API_KEY": self.api_key,
            "Accept": "application/json",
        })
        self.adls = adls_client or ADLSClient()
        self._credits_used = 0  # track within a single run

    # ── Internal helpers ─────────────────────────────────────────

    def _get(self, endpoint: str, params: dict = None, retries: int = 3) -> dict:
        """
        Make a GET request with retry logic and basic rate-limit handling.
        CMC returns 429 when rate limited.
        """
        url = f"{CMC_BASE_URL}{endpoint}"
        for attempt in range(1, retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=15)

                if response.status_code == 429:
                    wait = 60  # wait 60s on rate limit
                    logger.warning(f"Rate limited. Waiting {wait}s... (attempt {attempt})")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                logger.error(f"Request failed (attempt {attempt}/{retries}): {e}")
                if attempt == retries:
                    raise
                time.sleep(5 * attempt)  # exponential backoff

    def _save_to_bronze(self, data: dict, endpoint_name: str, filename_suffix: str) -> str:
        """Wrap raw API response with metadata and upload to Bronze layer."""
        ingestion_date = today_partition()

        # Envelope with metadata for traceability
        envelope = {
            "meta": {
                "source": "coinmarketcap",
                "endpoint": endpoint_name,
                "ingested_at": utc_now_iso(),
                "ingestion_date": ingestion_date,
                "record_count": len(data.get("data", data) if isinstance(data, dict) else data),
            },
            "raw": data,
        }

        timestamp_str = utc_now_iso().replace(":", "").replace("-", "").replace(".", "")[:15]
        filename = f"{endpoint_name}_{filename_suffix}_{timestamp_str}.json"
        blob_path = build_blob_path(
            source="coinmarketcap",
            endpoint=endpoint_name,
            ingestion_date=ingestion_date,
            filename=filename,
        )

        return self.adls.upload_json(container="bronze-prakhar", blob_path=blob_path, data=envelope)

    # ── Public methods ───────────────────────────────────────────

    def fetch_latest_listings(self) -> dict:
        """
        Fetch latest market data for top N coins.
        Endpoint: /v1/cryptocurrency/listings/latest
        Credit cost: ~1 per 200 coins fetched
        """
        logger.info(f"Fetching latest listings for top {self.top_n} coins...")

        params = {
            "limit": self.top_n,
            "convert": "USD",
            "sort": "market_cap",
            "sort_dir": "desc",
        }

        data = self._get("/v1/cryptocurrency/listings/latest", params=params)
        self._credits_used += CREDIT_COSTS["listings_latest"]

        blob_path = self._save_to_bronze(
            data=data,
            endpoint_name="listings_latest",
            filename_suffix=f"top{self.top_n}",
        )

        logger.info(f"Listings saved to Bronze: {blob_path}")
        logger.info(f"Credits used this run: {self._credits_used}")
        return data

    def fetch_quotes_latest(self, coin_ids: list[int]) -> dict:
        """
        Fetch real-time quotes for a specific list of coin IDs.
        Use this for selective refresh of key coins (BTC, ETH, etc.)
        Endpoint: /v1/cryptocurrency/quotes/latest
        Credit cost: 1 per 100 coins
        """
        if not coin_ids:
            logger.warning("No coin IDs provided for quotes fetch. Skipping.")
            return {}

        # CMC accepts comma-separated IDs
        id_str = ",".join(map(str, coin_ids))
        logger.info(f"Fetching quotes for {len(coin_ids)} coins: {id_str[:80]}...")

        params = {"id": id_str, "convert": "USD"}
        data = self._get("/v1/cryptocurrency/quotes/latest", params=params)
        self._credits_used += CREDIT_COSTS["quotes_latest"]

        blob_path = self._save_to_bronze(
            data=data,
            endpoint_name="quotes_latest",
            filename_suffix=f"{len(coin_ids)}coins",
        )

        logger.info(f"Quotes saved to Bronze: {blob_path}")
        return data

    def fetch_global_metrics(self) -> dict:
        """
        Fetch global market metrics (total market cap, BTC dominance, etc.)
        Endpoint: /v1/global-metrics/quotes/latest
        Credit cost: 1 (low frequency — call this sparingly)
        """
        logger.info("Fetching global market metrics...")
        data = self._get("/v1/global-metrics/quotes/latest", params={"convert": "USD"})
        self._credits_used += CREDIT_COSTS["global_metrics"]

        blob_path = self._save_to_bronze(
            data=data,
            endpoint_name="global_metrics",
            filename_suffix="latest",
        )

        logger.info(f"Global metrics saved to Bronze: {blob_path}")
        return data

    def get_credits_used(self) -> int:
        """Return total credits used in this client session."""
        return self._credits_used
import json
from azure.storage.blob import BlobServiceClient
from ingestion.utils import get_env, logger

class ADLSClient:
    """
    Handles all read/write operations to Azure Data Lake Storage Gen2.
    Uses Storage Account Key authentication.
    """
    def __init__(self):
        account_name = get_env("AZURE_STORAGE_ACCOUNT_NAME")
        account_key  = get_env("AZURE_STORAGE_ACCOUNT_KEY")
        
        account_url = f"https://{account_name}.blob.core.windows.net"
        self.client = BlobServiceClient(account_url=account_url, credential=account_key)
        
        logger.info(f"ADLSClient initialized for account: {account_name} using Account Key")

    def upload_json(self, container: str, blob_path: str, data: dict | list) -> str:
        """
        Serialize data to JSON and upload to the specified container/blob_path.
        Returns the full blob path on success.
        """
        json_bytes = json.dumps(data, indent=2, default=str).encode("utf-8")
        blob_client = self.client.get_blob_client(container=container, blob=blob_path)
        blob_client.upload_blob(json_bytes, overwrite=True)
        logger.info(f"Uploaded to {container}/{blob_path} ({len(json_bytes)} bytes)")
        return f"{container}/{blob_path}"

    def download_json(self, container: str, blob_path: str) -> dict | list:
        """Download and deserialize a JSON blob."""
        blob_client = self.client.get_blob_client(container=container, blob=blob_path)
        raw = blob_client.download_blob().readall()
        logger.info(f"Downloaded from {container}/{blob_path}")
        return json.loads(raw)

    def blob_exists(self, container: str, blob_path: str) -> bool:
        """Check if a blob already exists (useful to avoid re-ingesting)."""
        blob_client = self.client.get_blob_client(container=container, blob=blob_path)
        return blob_client.exists()

    def list_blobs(self, container: str, prefix: str = "") -> list[str]:
        """List all blob names under a given prefix."""
        container_client = self.client.get_container_client(container)
        return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]
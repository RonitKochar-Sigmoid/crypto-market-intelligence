Crypto Market Intelligence Pipeline
An end-to-end data engineering pipeline that ingests real-time cryptocurrency data from multiple APIs, processes it through a Medallion architecture in Databricks, and orchestrates the entire flow using Airflow running on Kubernetes (Minikube).

🏗 Project Architecture
Ingestion Layer: Python scripts (CMC, CoinGecko) containerized in Docker.

Storage Layer: Azure Data Lake Storage (ADLS) Gen2 (Bronze, Silver, Gold zones).

Transformation Layer: Databricks Spark notebooks (Validation -> Delta Lake -> Feature Engineering).

Orchestration Layer: Apache Airflow running on Minikube using KubernetesPodOperator.

🚀 1. Ingestion to ADLS (Bronze)
The ingestion layer extracts raw JSON data and lands it in the Bronze zone.

Sources:

CoinMarketCap: Real-time listings and global quotes.

CoinGecko: Market snapshots and 30-day historical backfills for BTC/ETH.

Logic: Data is timestamped and partitioned by ingestion_date to prevent data loss.

Containerization: Logic is packaged into the crypto-ingestion:v1 image.

Key Files: cmc_ingestor.py, coingecko_ingestor.py, Dockerfile.

💎 2. Databricks Medallion Workflow
The pipeline follows the Medallion architecture to ensure data quality and readiness for analytics.

Bronze Validation (01_bronze_validation.ipynb)
Loads raw JSON files from ADLS using Spark.

Validates payload integrity (checking for nulls or empty responses).

Flattens high-level metadata for downstream processing.

Silver Transformations (02_silver_transformations.ipynb)
Schema Enforcement: Applies strict schemas to the nested JSON payloads.

Deduplication: Removes duplicate records based on id and ingested_at.

Delta Lake: Saves data as Delta tables (silver_quotes, silver_historical, silver_global) for ACID compliance.

Gold Features (03_gold_transformations.ipynb)
Aggregations: Calculates 7-day and 30-day moving averages for prices.

Analytics: Creates high-level views of market dominance and price volatility.

Output: Final tables optimized for PowerBI or ML modeling.

☸️ 3. Airflow Setup & Scheduling
The orchestration runs locally on a Kubernetes cluster via Minikube.

Infrastructure Setup
Start Cluster: minikube start

Mount DAGs: Connect local files to the cluster:

Bash
minikube mount ./airflow_dags:/Users/as-mac-1250/crypto-market-intelligence/airflow_dags
Secrets: Kubernetes Secrets store the AZURE_STORAGE_CONNECTION_STRING.

Deploy:

Bash
kubectl apply -f infra/airflow-manifests.yaml
DAG Definitions (crypto_pipeline_dag.py)
cmc_real_time_ingestion: Runs every 15 minutes.

coingecko_on_demand_backfill: Manual trigger for historical data recovery.

daily_databricks_processing: Runs daily at 08:00 AM to process all Bronze data through to Gold.

🛠 Project Management Commands
Running the Pipeline Locally
Bash
# 1. Point terminal to Minikube's Docker
eval $(minikube docker-env)

# 2. Build Ingestion Image
docker build -t crypto-ingestion:v1 .

# 3. Retrieve Airflow Admin Password
kubectl exec -it <airflow-pod-name> -- cat /opt/airflow/standalone_admin_password.txt

# 4. Port Forward to Web UI
kubectl port-forward svc/airflow-webserver 8080:8080
Required Airflow Connections
databricks_default:

Conn Type: Databricks

Host: Your workspace URL (https://adb-xxx.azuredatabricks.net)

Password: Your Personal Access Token (PAT).

📂 Repository Structure
Plaintext
.
├── airflow_dags/
│   └── crypto_pipeline_dag.py    # Airflow DAG logic
├── databricks_notebooks/
│   ├── 01_bronze_validation.ipynb
│   ├── 02_silver_transformations.ipynb
│   └── 03_gold_transformations.ipynb
├── infra/
│   ├── airflow-manifests.yaml    # K8s Deployment & Service
│   └── ingestion-job.yaml        # Individual Job templates
├── cmc_ingestor.py               # CMC API logic
├── coingecko_ingestor.py         # CoinGecko API logic
└── Dockerfile                    # Ingestion image config
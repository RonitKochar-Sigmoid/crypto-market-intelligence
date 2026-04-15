# Crypto Market Cap Analytics Platform 🚀

An end-to-end data engineering pipeline that ingests, processes, and serves cryptocurrency market data. This project utilizes a Medallion Architecture (Bronze, Silver, Gold) to transform raw API data into analytical dashboards.

## 🏗️ Architecture & Tech Stack

* **Data Source:** CoinGecko API (Historical and Real-Time Market Data)
* **Orchestration:** Apache Airflow (Dockerized)
* **Storage:** Azure Data Lake Storage Gen2 (ADLS)
* **Data Processing:** Databricks & PySpark (Delta Lake)
* **Language:** Python 3.10+

---

## 🛠️ Prerequisites

Before you begin, ensure you have the following installed on your machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* [Python 3.10+](https://www.python.org/downloads/)
* Git
* An Azure account with a Data Lake Storage Gen2 account created.
* A CoinGecko API Key (Free tier works, Pro tier recommended for high volume).
* A CoinMarketCap API key

---

## 🚀 Phase 1: Local Environment Setup & Airflow Initialization

This initial phase covers cloning the repository, setting up an isolated Python environment, and getting the Airflow container running locally to handle our data ingestion.

**1. Clone the repository and navigate into it**
```bash
git clone <your-repo-url>
cd crypto-market-intelligence
```

**2. Create and activate a virtual environment**
It is highly recommended to use a virtual environment to keep project dependencies isolated.
```bash
# Create the virtual environment
python -m venv venv

# Activate the virtual environment:
# -> On macOS/Linux:
source venv/bin/activate
# -> On Windows:
venv\Scripts\activate
```

**3. Install project dependencies**
With your virtual environment activated, install the required Python packages.
```bash
# Upgrade pip to the latest version
pip install --upgrade pip

# Install the dependencies
pip install -r requirements.txt
```

**4. Configure Environment Variables**
Create your local `.env` file from the provided template. You will need to open this file and add your specific API keys and Azure credentials.
```bash
cp .env.example .env
```

**5. Start the Airflow Docker Containers**
We use Docker Compose to orchestrate the Airflow webserver, scheduler, and database.
```bash
# Initialize the Airflow database (Run this ONLY the first time)
docker-compose up airflow-init

# Start all Airflow services in detached mode
docker-compose up -d
```

**6. Access the Airflow UI**
* Open your browser and navigate to: `http://localhost:8082`
* **Username:** `admin`
* **Password:** `admin`

**7. Enable and Trigger DAGs**
Once inside the Airflow UI, you can interact with the pipelines:
* **`real_time_ingestion`**: Unpause this DAG to allow it to run automatically every 15 minutes.
* **`historical_backfill`**: Unpause and trigger this DAG manually **once** to seed the Data Lake with historical crypto data.

**8. Run Tests (Optional but recommended)**
Verify that your local logic and DAG definitions are valid before making new commits.
```bash
pytest tests/ -v
```

---

## 📂 Repository Structure

```text
crypto-platform/
│
├── airflow_dags/                           # Airflow DAG definitions
│   ├── historical_backfill.py      # DAG for 180-day backfill
│   └── real_time_ingestion.py      # DAG for 15-minute incremental loads
│
├── ingestion/                      # Core ingestion logic and API clients
│   ├── adls_client.py              # Azure Data Lake connector
│   ├── coingecko_client.py         # API client with rate-limiting & backoff
│   └── utils.py                    # Helper functions and formatting
│
├── .env.example                    # Template for environment variables
├── .gitignore                      # Git exclusions (logs, pycache, venv)
├── docker-compose.yaml             # Airflow infrastructure configuration
├── requirements.txt                # Python package dependencies
└── README.md                       # Project documentation
```

---

## 🔮 Phase 2: Databricks & Medallion Architecture (Coming Soon)
After the data is successfully ingested into the **Bronze Layer** (Raw JSON) via Airflow, Phase 2 will involve Databricks notebooks to:
1. Clean and type-cast the data into the **Silver Layer** (Delta Tables).
2. Pre-calculate business metrics (Moving Averages, Daily OHLC) into the **Gold Layer**.
3. Connect the Gold tables to a BI Dashboard.
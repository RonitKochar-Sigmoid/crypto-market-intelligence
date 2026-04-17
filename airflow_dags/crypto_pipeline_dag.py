from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

# Common settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# --- DAG 1: CMC Ingestion (Every 15 Minutes) ---
with DAG(
    dag_id='cmc_real_time_ingestion',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['ingestion', 'local']
) as dag_cmc:
    run_cmc = KubernetesPodOperator(
        task_id='run_cmc_ingestion',
        name='cmc-ingestion-job',
        namespace='default',
        image='crypto-ingestion:v1',
        cmds=["python", "cmc_ingestor.py"],
        in_cluster=True
    )

# --- DAG 2: CoinGecko (On Demand) ---
with DAG(
    dag_id='coingecko_on_demand_backfill',
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['ingestion', 'manual']
) as dag_gecko:
    run_gecko = KubernetesPodOperator(
        task_id='run_gecko_ingestion',
        name='coingecko-ingestion-job',
        namespace='default',
        image='crypto-ingestion:v1',
        cmds=["python", "coingecko_ingestor.py"],
        in_cluster=True
    )

# --- DAG 3: Databricks Pipeline (Daily 8 AM) ---
with DAG(
    dag_id='daily_databricks_processing',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['processing', 'databricks']
) as dag_db:
    trigger_db = DatabricksRunNowOperator(
        task_id='trigger_databricks_pipeline',
        databricks_conn_id='databricks_default',
        job_id=372835959529722
    )
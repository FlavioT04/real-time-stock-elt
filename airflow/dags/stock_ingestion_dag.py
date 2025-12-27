from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    'stock_ingestion',
    default_args={
        'owner': 'flavio',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Ingest stock prices and publish to kafka',
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 12, 27),
    catchup=False,
    tags=['stocks', 'etl'],
) as dag:

    ingest_task = BashOperator(
        task_id='ingest_stock_data',
        bash_command='python /Users/flavio/Documents/Projects/stock-etl-project/scripts/ingest.py'
    )
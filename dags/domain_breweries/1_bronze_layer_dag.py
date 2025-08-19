from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Importa a função de lógica do nosso outro arquivo!
from domain_breweries.scripts.bronze_ingestion import extract_from_dbbrewery_to_bronze

with DAG(
    dag_id='breweries_1_bronze_ingestion',
    start_date=datetime(2025, 8, 15),
    schedule='@daily',
    description='Daily ingestion from the API brewery to the bronze layer.',
    catchup=False,
    tags=['breweries', 'bronze', 'ingestion']
) as dag:

    ingest_task = PythonOperator(
        task_id='extract_data_from_db_brewery_api',
        python_callable=extract_from_dbbrewery_to_bronze,
    )
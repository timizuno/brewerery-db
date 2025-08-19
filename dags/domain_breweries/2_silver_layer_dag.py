from airflow.datasets import Dataset
from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from domain_breweries.scripts.silver_process import process_silver_layer

bronze_breweries_dataset = Dataset(f"local:///opt/airflow/data/bronze")
silver_breweries_dataset = Dataset(f'local://opt/airflow/data/silver/breweries')
with DAG(
    dag_id='breweries_2_silver_processing',
    start_date=datetime(2025, 8, 15),
    schedule=[bronze_breweries_dataset],
    description='Pipeline to transform the data with Pyspark from the bronze to the silver layer.',
    catchup=False,
    tags=['breweries', 'silver', 'pyspark'],
) as dag:

    process_task = PythonOperator(
        task_id='extract_data_from_db_brewery_api',
        python_callable=process_silver_layer,
        outlets=[silver_breweries_dataset]
    )
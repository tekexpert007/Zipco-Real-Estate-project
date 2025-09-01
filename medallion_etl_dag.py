from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'etl-user',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='local_medallion_etl',  # DAG name (shown in Airflow UI)
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    default_args=default_args,
    catchup=False,
) as dag:

    bronze = PythonOperator(
        task_id='bronze',
        python_callable=lambda: subprocess.run(['python', 'bronze_etl.py'], check=True)
    )

    silver = PythonOperator(
        task_id='silver',
        python_callable=lambda: subprocess.run(['python', 'silver_etl.py'], check=True)
    )

    gold = PythonOperator(
        task_id='gold',
        python_callable=lambda: subprocess.run(['python', 'gold_etl.py'], check=True)
    )

    bronze >> silver >> gold

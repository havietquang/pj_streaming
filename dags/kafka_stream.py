from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def stream_data_function():
    # Placeholder for streaming data logic
    print("Streaming data...")
    import requests
    import json
    response = requests.get('https://randomuser.me/api/')
    if response.status_code == 200:
        data = response.json()
        print("Data received:", data)

with DAG('user_automation',
         default_args=default_args,
         description='A simple user automation DAG',
         schedule='@daily',
         catchup=False) as dag:

    stream_data = PythonOperator(
        task_id='stream_data',
        python_callable=stream_data_function,
    )
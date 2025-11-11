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
    import json
    import requests
    from kafka import KafkaProducer
    import time
    import logging

    # Khởi tạo Kafka Producer (chỉ 1 lần, bên ngoài vòng lặp)
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],  # broker của Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Kafka Producer initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Kafka Producer: {e}")
        return  # Thoát nếu không kết nối được Kafka

    print("🔹 Starting data streaming for 60 seconds...")
    
    # Lấy thời gian bắt đầu
    curr_time = time.time()

    while True:
        # Điều kiện dừng: Chạy trong 60 giây
        if time.time() > curr_time + 60:
            print("🛑 60 seconds elapsed. Stopping stream.")
            break

        try:
            # Gọi API
            response = requests.get('https://randomuser.me/api/')
            
            if response.status_code == 200:
                data = response.json()
                
                producer.send('user_data', value=data)
                print("✅ Sent data to Kafka topic: user_data")
            
            else:
                logging.warning(f"Failed to fetch data, status_code={response.status_code}")

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            
        time.sleep(1) 

    producer.flush()
    producer.close()
    print("🔹 Data streaming finished.")


with DAG('user_automation',
         default_args=default_args,
         description='A simple user automation DAG',
         schedule='@daily',
         catchup=False) as dag:

    stream_data = PythonOperator(
        task_id='stream_data',
        python_callable=stream_data_function,
    )
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="submit_spark_streaming_job",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_job = SparkSubmitOperator(
    task_id="submit_spark_job",
    application="/opt/spark/apps/spark_streaming.py",
    conn_id="spark_standalone",
    verbose=True,
    conf={
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "client",
    },
    name="arrow-spark",
    packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
)


FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

ENV PATH="/opt/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir --upgrade \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-openlineage>=1.8.0 \
    pyspark==4.0.0 \
    kafka-python \
    cassandra-driver

    
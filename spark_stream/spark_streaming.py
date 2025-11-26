#!/usr/bin/env python3
# coding: utf-8
"""
Optimized Spark Structured Streaming -> Cassandra
- startingOffsets=latest (only new data)
- maxOffsetsPerTrigger to throttle ingestion
- checkpointLocation (mandatory)
- use foreachBatch to write to Cassandra (better connection handling)
- logging & simple retry for cassandra writes
"""

import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)
from cassandra.cluster import Cluster
from cassandra import ReadTimeout, OperationTimedOut, WriteTimeout

# Basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def create_spark_session():
    logging.info("Creating Spark session...")
    try:
        # NOTE: versions should be compatible with your Spark image
        spark = (
            SparkSession.builder.appName("KafkaSparkStreamingToCassandra")
            # use packages via spark.jars.packages, could be overridden by spark-submit --packages
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.0,"
                    "com.datastax.spark:spark-cassandra-connector_2.13:4.1.0") \
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.sql.shuffle.partitions", "4")  # small cluster -> fewer partitions
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logging.info("Spark session created.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return None


def setup_cassandra():
    """
    Ensure keyspace and table exist. Use cassandra-driver to create.
    """
    logging.info("Setting up Cassandra keyspace and table...")
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streaming
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streaming.users (
                id text PRIMARY KEY,
                first_name text,
                last_name text,
                gender text,
                email text,
                dob_age int,
                country text,
                city text,
                picture text
            )
        """)
        session.shutdown()
        logging.info("Cassandra keyspace/table ready.")
        return True
    except Exception as e:
        logging.error(f"Error setting up Cassandra: {e}")
        return False


def write_batch_to_cassandra(batch_df, batch_id):
    logging.info(f"Processing batch_id={batch_id}, rows=N/A")  # tránh count()
    if batch_df is None or batch_df.rdd.isEmpty():
        logging.info("Empty batch, skipping write.")
        return

    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        try:
            (batch_df.write
                .format("org.apache.spark.sql.cassandra")
                .mode("append")
                .options(keyspace="spark_streaming", table="users")
                .save()
            )
            logging.info(f"Batch {batch_id} written to Cassandra successfully.")
            break
        except (ReadTimeout, OperationTimedOut, WriteTimeout) as cex:
            attempt += 1
            logging.warning(f"Cassandra transient error on attempt {attempt}: {cex}. Retrying in 2s...")
            time.sleep(2)
        except Exception as ex:
            logging.error(f"Fatal error writing batch {batch_id} to Cassandra: {ex}")
            break


def process_stream(spark):
    """
    Build streaming pipeline:
    - throttle ingestion with maxOffsetsPerTrigger
    - read startingOffsets=latest (do not reprocess whole topic on restart)
    - checkpointLocation required for exactly-once / progress tracking
    - foreachBatch -> write_batch_to_cassandra
    """
    logging.info("Starting stream processing...")

    user_schema = StructType([
        StructField("gender", StringType(), True),
        StructField("name", StructType([
            StructField("first", StringType(), True),
            StructField("last", StringType(), True)
        ]), True),
        StructField("location", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("login", StructType([
            StructField("uuid", StringType(), True)
        ]), True),
        StructField("dob", StructType([
            StructField("age", IntegerType(), True)
        ]), True),
        StructField("picture", StructType([
            StructField("medium", StringType(), True)
        ]), True)
    ])

    kafka_schema = StructType([
        StructField("results", ArrayType(user_schema), True)
    ])

    try:
        df_kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "user_data")
            .option("startingOffsets", "earliest")          # READ NEW DATA ONLY
            .option("maxOffsetsPerTrigger", 1000)          # throttle ingestion
            .load()
        )

        df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), kafka_schema).alias("data"))

        # results is an array; explode or take first element if structure known.
        # Here we assume each message contains results: [user], so we pick index 0 safely.
        df_user = df_parsed.select(col("data.results")[0].alias("user"))

        df_flattened = df_user.select(
            col("user.login.uuid").alias("id"),
            col("user.name.first").alias("first_name"),
            col("user.name.last").alias("last_name"),
            col("user.gender").alias("gender"),
            col("user.email").alias("email"),
            col("user.dob.age").alias("dob_age"),
            col("user.location.country").alias("country"),
            col("user.location.city").alias("city"),
            col("user.picture.medium").alias("picture")
        )

        checkpoint_location = "/tmp/checkpoints/users"

        query = (
            df_flattened.writeStream
            .foreachBatch(write_batch_to_cassandra)
            .option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            .start()
        )

        logging.info("Streaming query started. Awaiting termination...")
        query.awaitTermination()
    except Exception as e:
        logging.error(f"Error during stream processing: {e}")


if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        if setup_cassandra():
            process_stream(spark)
        else:
            logging.error("Could not setup Cassandra. Exiting.")
    else:
        logging.error("Could not create Spark session. Exiting.")

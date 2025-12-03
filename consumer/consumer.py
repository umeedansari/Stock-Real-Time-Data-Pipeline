# requirements: pip install kafka-python boto3 botocore
import json
import time
import logging
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
load_dotenv()

#Define variables for API
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- MinIO (S3) connection ----------
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",  # host mapping in your compose (host:9002 -> container:9000)
    aws_access_key_id="admin",
    aws_secret_access_key=os.getenv('SECRET_KEY'),
    region_name="us-east-1",
    config=None
)

bucket_name = "stock-data"

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    logging.info(f"Bucket '{bucket_name}' already exists.")
except ClientError:
    try:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"Created bucket '{bucket_name}'.")
    except ClientError as e:
        logging.error("Failed to create bucket: %s", e)
        raise

# ---------- Kafka consumer ----------
# If running from host machine use "localhost:29092"
# If running from another container in the same compose network use "kafka:9092"
BOOTSTRAP = ["localhost:29092"]

consumer = KafkaConsumer(
    "stocks",                          # topic name — ensure producer uses same
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer1",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    consumer_timeout_ms=1000
)

logging.info("Consumer started — streaming and saving to MinIO...")

def save_record_to_s3(record):
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at", int(time.time()))
    key = f"{symbol}/{ts}.json"
    body = json.dumps(record).encode("utf-8")
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType="application/json"
        )
        logging.info("Saved record: s3://%s/%s", bucket_name, key)
    except ClientError as e:
        logging.error("S3 put_object failed: %s", e)
        raise

# ---------- Main loop ----------
while True:
    try:
        for message in consumer:
            try:
                record = message.value
                if not record:
                    logging.warning("Received empty message at offset %s", message.offset)
                    continue
                save_record_to_s3(record)
            except Exception as e:
                logging.exception("Failed processing message at offset %s: %s", getattr(message, "offset", "n/a"), e)
        # consumer timed out (consumer_timeout_ms), loop back to keep process alive
    except Exception as e:
        logging.exception("Kafka consumer error, will retry in 5s: %s", e)
        time.sleep(5)

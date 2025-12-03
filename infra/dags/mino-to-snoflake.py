import os
import json
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
load_dotenv()

MINIO_ENDPOINT = "http://minio:9000"       # use service name when running in compose
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = os.getenv('SECRET_KEY')
BUCKET = "stock-data"
LOCAL_DIR = "/tmp/minio_downloads"         # must be writable by Airflow container

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USERNAME')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD_SECRET')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT_SECRET')
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "stock"
SNOWFLAKE_SCHEMA = "stock_data"
SNOWFLAKE_TABLE = "stocks_table"   # table name used for COPY INTO

def download_from_minio(**kwargs):
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    resp = s3.list_objects_v2(Bucket=BUCKET)
    objects = resp.get("Contents") or []
    local_files = []

    for obj in objects:
        key = obj["Key"]
        # create a local name that avoids collisions (use full key name if needed)
        local_file = os.path.join(LOCAL_DIR, key.replace("/", "_"))
        try:
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files.append({"key": key, "local_path": local_file})
        except ClientError as e:
            print(f"Failed to download {key}: {e}")

    # Return list of dicts (Airflow will push this to XCom automatically)
    return local_files


def load_to_snowflake(ti, **kwargs):
    # Pull list returned by download task
    local_files = ti.xcom_pull(task_ids='download_minio') or []
    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    try:
        for entry in local_files:
            local_path = entry["local_path"]
            # Snowflake PUT requires file:// prefix and the file must be accessible from the container
            put_cmd = f"PUT file://{local_path} @%{SNOWFLAKE_TABLE} AUTO_COMPRESS=FALSE"
            print("Executing:", put_cmd)
            cur.execute(put_cmd)
            print(f"Uploaded {local_path} to Snowflake table stage")

        copy_cmd = f"""
            COPY INTO {SNOWFLAKE_TABLE}
            FROM @%{SNOWFLAKE_TABLE}
            FILE_FORMAT = (TYPE=JSON)
            ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_cmd)
        print("COPY INTO executed")

        # Optionally: cleanup local files or remove from MinIO after successful load
        # for entry in local_files:
        #     os.remove(entry['local_path'])

    finally:
        cur.close()
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minute
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
        provide_context=True,   # optional; kept for compatibility but not required in Airflow 2+
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task1 >> task2

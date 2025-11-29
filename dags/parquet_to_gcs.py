from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # type: ignore
from datetime import datetime
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import os

default_args = {
    "start_date": datetime(2025, 9, 1),
}

BUCKET_NAME = "airflow-testing-bucket-01"
PARQUET_FILE = "/tmp/output.parquet"
DESTINATION_BLOB_NAME = "data/output.parquet"

def generate_parquet():
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    })
    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_FILE)

def upload_to_gcs():
    hook = GCSHook(gcp_conn_id="GCP-TEST-DAG")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=DESTINATION_BLOB_NAME,
        filename=PARQUET_FILE,
        mime_type="application/octet-stream"
    )
    # Optional cleanup
    os.remove(PARQUET_FILE)

with DAG("parquet_to_gcs",
         default_args=default_args,
         catchup=False) as dag:

    task_generate = PythonOperator(
        task_id="generate_parquet",
        python_callable=generate_parquet
    )

    task_upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs
    )

    task_generate >> task_upload
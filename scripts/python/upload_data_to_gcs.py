import os
import time
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook # type: ignore
from airflow.models import Variable # type: ignore

def upload_parquet_to_gcs(**context):
    start = time.time()
    logical_date = context["logical_date"]
    file_date = logical_date - timedelta(days=1) # Data file day -1
    date_str = file_date.strftime("%Y%m%d")

    # Local Parquet file location
    local_file = f"/opt/airflow/data/ga4data_{date_str}.parquet"

    # Check file exists
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"[ERROR] Parquet not found: {local_file}")

    # GCS bucket
    bucket_name = Variable.get("GCS_BUCKET_NAME")
    object_name = f"data-ga4/ga4data_{date_str}.parquet"


    print(f"Uploading {local_file} > gs://{bucket_name}/{object_name}")

    # Upload using GCSHook
    hook = GCSHook(gcp_conn_id="GCP-TEST-DAG")

    hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        filename=local_file,
        mime_type="application/octet-stream"
    )


    upload_time = round(time.time() - start, 2)

    # push upload_time to XCom
    context["ti"].xcom_push(key="upload_time", value=upload_time)
    print(f"✅ Upload complete Took {upload_time} seconds.")

    print("✅ Upload complete!")
    context["ti"].xcom_push(key = "upstream_task_status", value = "success")
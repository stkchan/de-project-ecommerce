import os
import time
import tempfile
from datetime import timedelta

from airflow.providers.google.cloud.hooks.gcs import GCSHook  # type: ignore
from azure.storage.blob import BlobServiceClient  # type: ignore
from airflow.models import Variable  # type: ignore


def upload_from_gcs_to_azure(**context):
    start_time      = time.time()

    logical_date    = context["logical_date"]
    file_date       = logical_date - timedelta(days=1)
    date_str        = file_date.strftime("%Y%m%d")

    # -----------------------------
    # 1) Source file in GCS
    # -----------------------------
    bucket_name     = Variable.get("GCS_BUCKET_NAME")
    gcs_object      = f"data-ga4/ga4data_{date_str}.parquet"

    print(f"Downloading from GCS: gs://{bucket_name}/{gcs_object}")

    gcs_hook = GCSHook(gcp_conn_id="GCP-TEST-DAG")

    # ทำ temp file
    """"
    ทำไมต้องใช้ temp file?
    เพราะ Airflow + GCSHook + Azure Blob SDK ไม่ได้รองรับการ stream ไฟล์ข้าม cloud โดยตรง
    จึงต้อง “ดาวน์โหลดมาก่อน” → แล้วค่อย “อัปโหลดขึ้น Azure”

    """

    temp_local_path = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name

    # download from GCS
    gcs_hook.download(
        bucket_name = bucket_name,
        object_name = gcs_object,
        filename    = temp_local_path
    )

    print(f"Downloaded to temp file: {temp_local_path}")

    # -----------------------------
    # 2) Upload to Azure
    # -----------------------------
    conn_str        = Variable.get("AZURE_STORAGE_CONNECTION")
    container_name  = Variable.get("AZURE_CONTAINER_BRONZE")

    # Destination path (Bronze)
    azure_blob_path = (
        f"ga4/year={file_date.year}/"
        f"month={file_date.month:02d}/"
        f"day={file_date.day:02d}/"
        f"ga4_{date_str}.parquet"
    )

    print(f"Uploading → Azure: {container_name}/{azure_blob_path}")

    blob_service    = BlobServiceClient.from_connection_string(conn_str)
    blob_client     = blob_service.get_blob_client(container=container_name, blob=azure_blob_path)

    with open(temp_local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    upload_time = round(time.time() - start_time, 2)

    # Push XCom
    ti = context["ti"]
    ti.xcom_push(key = "upload_time_azure",    value = upload_time)
    ti.xcom_push(key = "upstream_task_status", value = "success")

    print(f"✅ Uploaded to Azure Storage in {upload_time} sec")

    # Cleanup temp file
    os.remove(temp_local_path)

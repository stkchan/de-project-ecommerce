from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import pendulum # type: ignore
import sys
from pathlib import Path
import os

# Add scripts path
sys.path.append(str(Path("/opt/airflow/scripts/python")))

# Import functions from Python files
from dag_notification import generate_data_wrapper, notify_slack_task, notify_gmail_task, notify_slack_upload_task, notify_gmail_upload_task, notify_slack_azure_task, notify_gmail_azure_task # type: ignore
from upload_data_to_gcs import upload_parquet_to_gcs # type: ignore
from upload_from_gcs_to_azure import upload_from_gcs_to_azure # type: ignore

# Send notification to email
emails = [
    os.getenv("ALERT_EMAIL_1"),
    os.getenv("ALERT_EMAIL_2")
]

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [email for email in emails if email],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id       = "DE-Project-Ecommerce",
    description  = "Generate GA4 data & Sending Notifications (Slack, Discord, Email) & Store data to Postgres",
    default_args = default_args,
    start_date   = datetime(2025, 10, 6, tzinfo = local_tz),
    schedule     = "0 9 * * *",
    catchup      = False,
    tags         = ["GA4", "Slack", "Email", "Postgres"]
) as dag:

    generate_ga4_data = PythonOperator(
        task_id="generate_ga4_data",
        python_callable=generate_data_wrapper
    )

    notification_slack_generate_data = PythonOperator(
        task_id="notification_slack_generate_data",
        python_callable=notify_slack_task
    )

    notification_gmail_generate_data = PythonOperator(
        task_id="notification_gmail_generate_data",
        python_callable=notify_gmail_task
    )

    upload_data_to_gcs = PythonOperator(
    task_id="upload_data_to_gcs",
    python_callable=upload_parquet_to_gcs
    )

    notification_slack_upload = PythonOperator(
    task_id="notification_slack_upload",
    python_callable=notify_slack_upload_task
    )

    notification_gmail_upload = PythonOperator(
    task_id="notification_gmail_upload",
    python_callable=notify_gmail_upload_task
    )

    upload_data_to_azure = PythonOperator(
    task_id="upload_data_to_azure",
    python_callable=upload_from_gcs_to_azure
    )

    notification_slack_azure = PythonOperator(
    task_id="notification_slack_upload_azure",
    python_callable=notify_slack_azure_task
    )

    notification_gmail_azure = PythonOperator(
        task_id="notification_gmail_upload_azure",
        python_callable=notify_gmail_azure_task
    )

    # DAG structure
    # generate_ga4_data >> [notification_slack_generate_data, notification_gmail_generate_data] >> upload_data_to_gcs >> [notification_slack_upload, notification_gmail_upload] >> upload_data_to_azure
    generate_ga4_data \
    >> [notification_slack_generate_data, notification_gmail_generate_data] \
    >> upload_data_to_gcs \
    >> [notification_slack_upload, notification_gmail_upload] \
    >> upload_data_to_azure \
    >> [notification_slack_azure, notification_gmail_azure]
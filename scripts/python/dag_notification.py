import subprocess
import pendulum  # type: ignore
import json
from pathlib import Path
from airflow.utils.state import State  # type: ignore
from apps_notification import send_slack_notification, send_email_notification


# -----------------------------------
# 1️⃣ Run GA4 Data Generator
# -----------------------------------
def generate_data_wrapper(**context):
    """Run GA4 data generator and push status to XCom."""
    try:
        result = subprocess.run(
            ["python", "/opt/airflow/scripts/python/ga4_generate_data.py", "--out-dir", "/opt/airflow/data"],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        context["ti"].xcom_push(key = "upstream_task_status", value = State.SUCCESS)

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Data generation failed: {e.stderr}")
        context["ti"].xcom_push(key = "upstream_task_status", value = State.FAILED)
        raise


# Helper: common context extractor
def extract_common_context(context, task_name):
    ti        = context["task_instance"]
    dag_name  = context["dag"].dag_id
    log_url   = ti.log_url
    date_str  = context["ds"]

    utc_dt    = pendulum.parse(context["ts"])
    th_dt     = utc_dt.in_timezone("Asia/Bangkok")
    exec_time = th_dt.format("YYYY-MM-DD HH:mm:ss")

    # -----------------------------------------
    # Case 1: notification for generate_data
    # -----------------------------------------
    if "generate" in task_name:
        status = ti.xcom_pull(task_ids="generate_ga4_data", key="upstream_task_status")
        status = str(status).replace("State.", "").lower() if status else "unknown"

        metrics_path = Path("/opt/airflow/data/last_run_metrics.json")
        total_rows = generation_time = None

        if metrics_path.exists():
            with open(metrics_path, "r") as f:
                data = json.load(f)
                total_rows = data.get("total_rows")
                generation_time = data.get("generation_time")

        return {
            "dag_name": dag_name,
            "task_name": task_name,
            "exec_time": exec_time,
            "date_str": date_str,
            "status": status,
            "total_rows": total_rows,
            "generation_time": generation_time,
            "log_url": log_url
        }

    # -----------------------------------------
    # Case 2: notification for upload_to_gcs
    # -----------------------------------------
    if task_name in [
        "notification_slack_upload_data_to_gcs",
        "notification_gmail_upload_data_to_gcs"
    ]:
        
        status = "success"

        upload_time = ti.xcom_pull(task_ids="upload_data_to_gcs", key = "upload_time")
        upload_time = upload_time if upload_time else "N/A"

        return {
            "dag_name": dag_name,
            "task_name": task_name,
            "exec_time": exec_time,
            "date_str": date_str,
            "status": status,
            "total_rows": None,
            "upload_time": upload_time,
            "log_url": log_url
        }

    # -----------------------------------------
    # Case 3: notification for upload_data_to_azure
    # -----------------------------------------
    if task_name in [
        "notification_slack_upload_gcs_to_azure",
        "notification_gmail_upload_gcs_to_azure"
    ]:
        
        status = "success"

        upload_time_azure = ti.xcom_pull(task_ids="upload_data_to_azure", key="upload_time_azure")
        upload_time_azure = upload_time_azure if upload_time_azure else "N/A"

        return {
            "dag_name": dag_name,
            "task_name": task_name,
            "exec_time": exec_time,
            "date_str": date_str,
            "status": status,
            "total_rows": None,
            "upload_time": upload_time_azure,
            "log_url": log_url
        }


# -----------------------------------
# 2️⃣ Notify Slack
# -----------------------------------
def notify_slack_task(**context):
    """Send notification to Slack only."""
    ctx = extract_common_context(context, task_name="notification_slack_generate_data")
    print(f"[DEBUG] Sending Slack notification: {ctx['status']}")
    try:
        send_slack_notification(**ctx)
    except Exception as e:
        print(f"[ERROR] Slack send failed: {e}")
        raise


# -----------------------------------
# 3️⃣ Notify Gmail
# -----------------------------------
def notify_gmail_task(**context):
    """Send notification to Gmail only."""
    ctx = extract_common_context(context, task_name="notification_gmail_generate_data")
    print(f"[DEBUG] Sending Gmail notification: {ctx['status']}")
    try:
        to_emails = context["dag"].default_args.get("email", [])
        if to_emails:
            send_email_notification(
                to_emails=to_emails,
                **ctx
            )
        else:
            print("[INFO] No alert emails configured — skipping Gmail notification.")
    except Exception as e:
        print(f"[ERROR] Email send failed: {e}")
        raise

def notify_slack_upload_task(**context):
    """Send Slack notification after upload to GCS."""
    ctx = extract_common_context(context, task_name = "notification_slack_upload_data_to_gcs")

    print(f"[DEBUG] Sending Slack upload notification: {ctx['status']}")
    try:
        send_slack_notification(**ctx)
    except Exception as e:
        print(f"[ERROR] Slack upload notification failed: {e}")
        raise


def notify_gmail_upload_task(**context):
    """Send Email notification after GCS upload."""
    ctx = extract_common_context(context, task_name = "notification_gmail_upload_data_to_gcs")

    print(f"[DEBUG] Sending Gmail upload notification: {ctx['status']}")
    try:
        to_emails = context["dag"].default_args.get("email", [])
        if to_emails:
            send_email_notification(
                to_emails=to_emails,
                **ctx
            )
        else:
            print("[INFO] No alert emails configured — skipping Gmail upload notification.")
    except Exception as e:
        print(f"[ERROR] Email upload notification failed: {e}")
        raise


def notify_slack_azure_task(**context):
    """Send Slack notification after uploading to Azure Storage."""
    ctx = extract_common_context(context, task_name = "notification_slack_upload_gcs_to_azure")

    print(f"[DEBUG] Sending Slack Azure upload notification: {ctx['status']}")

    try:
        send_slack_notification(**ctx)
    except Exception as e:
        print(f"[ERROR] Slack Azure upload notification failed: {e}")
        raise

def notify_gmail_azure_task(**context):
    """Send Gmail notification after uploading to Azure Storage."""
    ctx = extract_common_context(context, task_name="notification_gmail_upload_gcs_to_azure")

    print(f"[DEBUG] Sending Gmail Azure upload notification: {ctx['status']}")
    try:
        emails = context["dag"].default_args.get("email", [])
        if emails:
            send_email_notification(to_emails=emails, **ctx)
        else:
            print("[INFO] No emails configured — skipping Gmail Azure notification.")
    except Exception as e:
        print(f"[ERROR] Gmail Azure upload notification failed: {e}")
        raise
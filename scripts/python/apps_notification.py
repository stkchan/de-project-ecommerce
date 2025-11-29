import requests # type: ignore
from airflow.hooks.base import BaseHook  # type: ignore
from airflow.utils.email import send_email_smtp # type: ignore

def send_slack_notification(status, dag_name, task_name, exec_time, date_str, total_rows, generation_time=None, upload_time=None, log_url=None):
    """Send formatted Slack message via Slack API token stored in Airflow Connection."""
    try:
        # Get connection from Airflow (Connection ID: slack_conn)
        conn = BaseHook.get_connection("slack_conn")
        slack_token = conn.password
        channel = conn.extra_dejson.get("channel", "#alerts")

        emoji = "✅" if status.lower() == "success" else "❌"

        if upload_time:
            time_text = f"*Upload Time:* {upload_time} sec"
        else:
            time_text = f"*Generation Time:* {generation_time or 'N/A'} sec"

        text = (
            f"{emoji} *Pipeline: Ecommerce*\n"
            f"*Status:* {status.upper()}\n"
            f"*DAG:* {dag_name}\n"
            f"*Task:* {task_name}\n"
            f"*Execution Time:* {exec_time}\n"
            f"*Date:* {date_str}\n"
            f"*Total Rows:* {total_rows or 'N/A'}\n"
            f"{time_text}\n"
            f"<{log_url}|View Airflow Logs>"
        )

        headers = {"Authorization": f"Bearer {slack_token}"}
        payload = {"channel": channel, "text": text, "mrkdwn": True}
        response = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)

        if response.status_code != 200 or not response.json().get("ok"):
            print(f"[ERROR] Slack API response: {response.text}")
        else:
            print(f"[INFO] Slack notification sent successfully to {channel}")

    except Exception as e:
        print(f"[ERROR] Slack send failed: {e}")
        raise



def send_email_notification(to_emails, status, dag_name, task_name, exec_time, date_str, total_rows, generation_time=None, upload_time=None, log_url=None):
    subject = f"[Airflow] {dag_name} - {status.upper()}"

    if upload_time:
        time_line = f"<p><b>Upload Time:</b> {upload_time} sec</p>"
    else:
        time_line = f"<p><b>Generation Time:</b> {generation_time or 'N/A'} sec</p>"

    body = f"""
    <h3>GA4 Pipeline Notification</h3>
    <p><b>Status:</b> {status.upper()}</p>
    <p><b>DAG:</b> {dag_name}</p>
    <p><b>Task:</b> {task_name}</p>
    <p><b>Execution Time (TH):</b> {exec_time}</p>
    <p><b>Date:</b> {date_str}</p>
    <p><b>Total Rows:</b> {total_rows or 'N/A'}</p>
    <p><b>Time:</b> {time_line} sec</p>
    <p><a href="{log_url}">View Airflow Logs</a></p>
    """

    try:
        send_email_smtp(to=to_emails, subject=subject, html_content=body)
        print(f"[INFO] Email notification sent to {to_emails}")
    except Exception as e:
        print(f"[ERROR] Email send failed: {e}")
        raise

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from airflow.operators.email import EmailOperator
import os
from datetime import timedelta

API_TOKEN = os.getenv("API_TOKEN")
AZURE_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


def fetch_product_usage(start_date, end_date, blob_path):
    """
    Calls the product usage API for a date range
    and writes each day's response to Azure Blob Storage.
    """

    import requests
    from azure.storage.blob import BlobClient
    import json

    current = start_date

    while current <= end_date:

        # Convert date object to ISO string
        date_str = current.isoformat()

        # Build daily endpoint
        url = f"https://api.company.com/product-usage?date={date_str}"

        # API call
        response = requests.get(url, headers={"Authorization": f"Bearer {API_TOKEN}"})
        data = response.json()

        # Normalize rows
        rows = []
        for record in data.get("usage", []):
            rows.append(
                {
                    "company_id": record.get("company_id"),
                    "date": record.get("date", date_str),
                    "active_users": record.get("active_users", 0),
                    "events": record.get("events", 0),
                }
            )

        # Upload to Blob as JSON
        blob_name = f"{blob_path}/usage_{date_str}.json"
        blob = BlobClient.from_connection_string(
            AZURE_CONN, container="staging", blob_name=blob_name
        )

        blob.upload_blob(json.dumps(rows), overwrite=True)

        # Next day
        current += timedelta(days=1)


def download_crm_from_s3(**context):
    s3 = S3Hook(aws_conn_id="aws_default")
    bucket = "crm-bucket"
    key = f"crm/daily/crm_{context['ds']}.csv"

    local_path = f"/tmp/crm_{context['ds']}.csv"
    s3.download_file(bucket, key, local_path)

    return local_path


def run_api_ingestion(**context):
    # Airflow passes execution date automatically
    exec_date = context["ds"]  # string: "2025-11-30"
    start = datetime.fromisoformat(exec_date)
    end = start

    # Call your ingestion function
    fetch_product_usage(
        start_date=start,
        end_date=end,
        blob_path="product_usage/daily",
    )


with DAG(
    dag_id="company_activity_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    download_crm = PythonOperator(
        task_id="download_crm", python_callable=download_crm_from_s3
    )

    load_crm = MySqlOperator(
        task_id="load_crm",
        mysql_conn_id="mysql_local",
        sql="extras/sql/stage_load_crm.sql",
    )

    fetch_api = PythonOperator(
        task_id="fetch_usage", python_callable=run_api_ingestion, provide_context=True
    )

    merge = MySqlOperator(
        task_id="merge_transform",
        mysql_conn_id="mysql_local",
        sql="extras/sql/merge_transform.sql",
    )
    email_success = EmailOperator(
        task_id="email_on_success",
        to="example@example.com",
        subject="DAG Success - Airflow",
        html_content="<h3>The DAG finished successfully!</h3>",
        trigger_rule="all_success",  # only if everything succeeds
    )

    # Email on FAILURE
    email_failure = EmailOperator(
        task_id="email_on_failure",
        to="example@example.com",
        subject="DAG Failed - Airflow",
        html_content="<h3>The DAG failed!</h3>",
        trigger_rule="one_failed",  # run if any upstream fails
    )

    [download_crm >> load_crm, fetch_api] >> merge >> [email_success, email_failure]

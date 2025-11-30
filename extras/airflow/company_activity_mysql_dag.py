from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

default_args = {"owner": "ahmed", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="mysql_company_activity_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1. Create tables if not exist
    create_schema = MySqlOperator(
        task_id="create_schema",
        mysql_conn_id="mysql_local",
        sql="extras/mysql/create_schema.sql",
    )

    # 2. Load sample data
    load_sample_data = MySqlOperator(
        task_id="load_sample_data",
        mysql_conn_id="mysql_local",
        sql="extras/mysql/sample_data.sql",
    )

    # 3. Run the transformation
    run_transform = MySqlOperator(
        task_id="run_transform",
        mysql_conn_id="mysql_local",
        sql="extras/mysql/run_transform.sql",
    )

    create_schema >> load_sample_data >> run_transform

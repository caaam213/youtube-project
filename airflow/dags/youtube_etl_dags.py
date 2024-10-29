import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


sys.path.append("/opt/airflow")
from tasks.youtube_extraction import extract_youtube_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,  # Define if it depends on past DAG runs
    "start_date": datetime(2024, 10, 29, 0, 0, 0),  # Run everyday
    "email_on_failure": False,  # Do not send email if something went wrong
    "email_on_retry": False,  # Do not send email on retry
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # Delay between retries
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    "youtube_etl_dags",
    default_args=default_args,
    description="A DAG which runs every day at midnight to scrap Youtube Data",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Data extraction
extraction_task = PythonOperator(
    task_id="extract_data_from_youtube",
    python_callable=extract_youtube_data,
    op_kwargs={
        "api_key": os.getenv("YOUTUBE_API_KEY"),
        "keyword": "learn german",
    },  # TODO : Create a file where keywords are
    dag=dag,
)

extraction_task

import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


sys.path.insert(0, "/opt/airflow/")
from tasks import youtube_transform, youtube_extraction, youtube_load

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

keyword = "learn german"

# Data extraction
extraction_task = PythonOperator(
    task_id="extract_data_from_youtube",
    python_callable=youtube_extraction.extract_youtube_data,
    op_kwargs={
        "api_key": os.getenv("YOUTUBE_API_KEY"),
        "keyword": keyword,
    },  # TODO : Create a file where keywords are
    dag=dag,
)

# Data transformation
transformation_task = PythonOperator(
    task_id="transform_youtube_data",
    python_callable=youtube_transform.process_data,
    op_kwargs={
        "keyword": keyword,
    },
    dag=dag,
)

# Load data
load_task = PythonOperator(
    task_id="load_youtube_data",
    python_callable=youtube_load.load_data_on_s3,
    op_kwargs={"keyword": keyword, "bucket": "youtube-bucket-cam"},
    dag=dag,
)

extraction_task >> transformation_task >> load_task

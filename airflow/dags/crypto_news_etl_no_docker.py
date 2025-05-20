from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Paths to your scripts
SCRAPER_SCRIPT = "/path/to/scraper.py"
CLEANER_SCRIPT = "/path/to/clean.py"

def run_script(script_path):
    subprocess.run(["python", script_path], check=True)

with DAG(
    dag_id="crypto_news_etl_weekly_no_docker",
    default_args=default_args,
    description="Weekly ETL for crypto news without Docker",
    schedule_interval="@weekly",
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=["crypto", "python", "glue"],
) as dag:

    scrape_news = PythonOperator(
        task_id="scrape_news",
        python_callable=run_script,
        op_args=[SCRAPER_SCRIPT],
    )

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=run_script,
        op_args=[CLEANER_SCRIPT],
    )

    glue_job = AwsGlueJobOperator(
        task_id="run_glue_job",
        job_name="your_glue_job_name", 
        script_location="s3://your-bucket/scripts/glue_transform.py",  
        iam_role_name="your-glue-iam-role",
        region_name="us-east-1",
    )

    scrape_news >> clean_data >> glue_job

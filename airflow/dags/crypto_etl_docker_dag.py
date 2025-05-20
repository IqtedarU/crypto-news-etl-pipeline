from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_news_etl_weekly",
    default_args=default_args,
    description="Weekly ETL for crypto news using Docker and Glue",
    schedule_interval="@weekly",
    start_date=datetime(2024, 5, 5),
    catchup=False,
    tags=["crypto", "docker", "glue"],
) as dag:

    scrape_news = DockerOperator(
        task_id="scrape_news",
        image="your-dockerhub-username/crypto-scraper:latest",
        api_version="auto",
        auto_remove=True,
        command="python scraper.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    clean_data = DockerOperator(
        task_id="clean_data",
        image="your-dockerhub-username/crypto-cleaner:latest",
        api_version="auto",
        auto_remove=True,
        command="python clean.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    glue_job = AwsGlueJobOperator(
        task_id="run_glue_job",
        job_name="your_glue_job_name",
        script_location="s3://your-bucket/scripts/glue_transform.py",  
        iam_role_name="your-glue-iam-role",
        region_name="us-east-1"
    )

    scrape_news >> clean_data >> glue_job

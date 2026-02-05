import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "description": "Clean task execution with Docker for different tags",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}



TAGS = ["apache-spark", "python-polars", "sql", "pyspark", "dbt", "docker", "airflow"]

# Get AWS credentials from environment variables
aws_env = {
    "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_DEFAULT_REGION": Variable.get("AWS_DEFAULT_REGION", "us-east-1"),
}




with DAG(
    "clean_docker_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    # Create a task for each tag
    clean_tasks = []
    for tag in TAGS:
        task = DockerOperator(
            task_id=f"clean_{tag.replace('-', '_')}",
            image="clean_task_image_local",
            api_version="auto",
            auto_remove="force",
            command=f"python3 -m capstonellm.tasks.clean -t {tag}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            environment=aws_env,
        )
        clean_tasks.append(task)
        start_dag >> task

    # All tasks flow to end
    for task in clean_tasks:
        task >> end_dag

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

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

with DAG(
    "clean_docker_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    # Create a task for each tag
    clean_tasks = []
    for tag in TAGS:
        task = DockerOperator(
            task_id=f"clean_{tag.replace('-', '_')}",
            image="capstone_llm_clean",
            container_name=f"clean_task_image_local",
            api_version="auto",
            auto_remove="force",
            command=f"python -m capstonellm.tasks.clean -t {tag}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
        )
        clean_tasks.append(task)
        start_dag >> task

    # All tasks flow to end
    for task in clean_tasks:
        task >> end_dag

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

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

def get_aws_env_vars(conn_id="aws_default"):
    try:
        # Fetch the connection object
        conn = BaseHook.get_connection(conn_id)
        
        # Build the dictionary
        env_vars = {
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.password,
            "AWS_DEFAULT_REGION": conn.extra_dejson.get("region_name", "us-east-1")
        }
        
        # Handle temporary credentials (if using STS/Role assumption)
        if conn.extra_dejson.get("aws_session_token"):
             env_vars["AWS_SESSION_TOKEN"] = conn.extra_dejson.get("aws_session_token")
             
        return env_vars
    except Exception as e:
        print(f"Warning: Could not fetch AWS connection {conn_id}: {e}")
        return {}

TAGS = ["apache-spark", "python-polars", "sql", "pyspark", "dbt", "docker", "airflow"]

aws_env = get_aws_env_vars("aws_default")

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
            image="clean_task_image_local",
            api_version="auto",
            auto_remove="force",
            command=f"python3 -m capstonellm.tasks.clean -t {tag}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
        )
        clean_tasks.append(task)
        start_dag >> task

    # All tasks flow to end
    for task in clean_tasks:
        task >> end_dag

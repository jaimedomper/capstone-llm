from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from conveyor.operators import ConveyorContainerOperatorV2

default_args = {
    "owner": "jaime",
    "description": "Clean task execution with Conveyor for different tags",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TAGS = ["apache-spark", "python-polars", "sql", "pyspark", "dbt", "docker", "airflow"]

with DAG(
    "clean_conveyor_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    # Create a task for each tag
    for tag in TAGS:
        task = ConveyorContainerOperatorV2(
            dag=dag,
            task_id=f"clean_{tag.replace('-', '_')}",
            instance_type="mx.medium",
            aws_role="capstone_conveyor_llm",
            # Conveyor splits the command and arguments list
            cmds=["python3"],
            arguments=["-m", "capstonellm.tasks.clean", "-t", tag, "--env", "conveyor"], 
        )
        
        start_dag >> task >> end_dag
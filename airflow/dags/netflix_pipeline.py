import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

POSTGRES_ENV = {
    name: os.environ[name]
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )
}

with DAG(
    dag_id='netflix_pipeline_v3.3.6',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['spark_pipelines', 'netflix', 'etl'],
) as dag:
    bronze = DockerOperator(
        task_id="bronze",
        mount_tmp_dir=False,
        image="spark-etl-spark:local",
        command="""
    /opt/spark/bin/spark-submit
    --master local[*]
    /app/spark_pipelines/batch_pipelines/movies_pipeline/bronze/main.py
    """,
        network_mode="pythonproject_default",
        environment=POSTGRES_ENV,
        auto_remove="success",
    )

    silver = DockerOperator(
        task_id="silver",
        mount_tmp_dir=False,
        image="spark-etl-spark:local",
        command="""
    /opt/spark/bin/spark-submit
    --master local[*]
    /app/spark_pipelines/batch_pipelines/movies_pipeline/silver/main.py
    """,
        network_mode="pythonproject_default",
        environment=POSTGRES_ENV,
        auto_remove="success",
    )

    bronze >> silver

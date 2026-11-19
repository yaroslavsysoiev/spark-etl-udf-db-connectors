import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

from docker.types import Mount

POSTGRES_ENV = {
    name: os.environ[name]
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )
}
PIPELINE_DOCKER_NETWORK = os.environ["PIPELINE_DOCKER_NETWORK"]
PIPELINE_DATA_DIR = os.environ["PIPELINE_DATA_DIR"]


def pipeline_mounts():
    return [
        Mount(
            source=PIPELINE_DATA_DIR,
            target="/app/data",
            type="bind",
        )
    ]
with DAG(
    dag_id='netflix_pipeline_v3.3.6',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['spark_pipelines', 'netflix', 'etl'],
) as dag:
    bronze = DockerOperator(
        task_id="bronze",
        image="spark-etl-spark:local",
        command=[
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[*]",
            "/app/spark_pipelines/batch_pipelines/movies_pipeline/bronze/main.py",
        ],
        mount_tmp_dir=False,
        network_mode=PIPELINE_DOCKER_NETWORK,
        mounts=pipeline_mounts(),
        environment=POSTGRES_ENV,
        auto_remove="success",
    )

    silver = DockerOperator(
        task_id="silver",
        image="spark-etl-spark:local",
        command=[
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[*]",
            "/app/spark_pipelines/batch_pipelines/movies_pipeline/silver/main.py",
        ],
        mount_tmp_dir=False,
        network_mode=PIPELINE_DOCKER_NETWORK,
        mounts=pipeline_mounts(),
        environment=POSTGRES_ENV,
        auto_remove="success",
    )
    bronze >> silver

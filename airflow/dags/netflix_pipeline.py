from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id='netflix_pipeline_v3.2.9',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['spark_pipelines', 'netflix', 'etl'],
) as dag:
    bronze = BashOperator(
        task_id="bronze",
        bash_command="""
        set -e

        docker run --rm \
          --network pythonproject_default \
          -e POSTGRES_HOST=postgres \
          --env-file /workspace/.env \
          pythonproject-bronze:latest \
          /opt/spark/bin/spark-submit \
          --master local[*] \
          /app/spark_pipelines/movies_pipeline/bronze/main.py
        """
    )

    silver = BashOperator(
        task_id="silver",
        bash_command="""
        set -e

        docker run --rm \
          --network pythonproject_default \
          -e POSTGRES_HOST=postgres \
          --env-file /workspace/.env \
          pythonproject-silver:latest \
          /opt/spark/bin/spark-submit \
          --master local[*] \
          /app/spark_pipelines/movies_pipeline/silver/main.py
        """
    )

    bronze >> silver

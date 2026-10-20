from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id='netflix_pipeline_v3.2.4',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'netflix', 'etl'],
) as dag:
    bronze = BashOperator(
        task_id='bronze',
        bash_command="""
            docker run --rm \
            --network pythonproject_default \
            -e POSTGRES_HOST=postgres \
            --env-file /workspace/.env \
            pythonproject-bronze:latest
        """,
    )

    silver = BashOperator(
        task_id='silver',
        bash_command="""
            echo "START $(date)"
        
        docker run --rm \
        --network pythonproject_default \
        -e POSTGRES_HOST=postgres \
        --env-file /workspace/.env \
        pythonproject-silver:latest

        echo "END $(date)"
        """,
    )

    bronze >> silver

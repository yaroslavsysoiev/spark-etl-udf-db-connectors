from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='netflix_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'netflix', 'etl'],
) as dag:
    bronze = BashOperator(
        task_id='bronze',
        bash_command="echo 'Running Bronze'",
    )

    silver = BashOperator(
        task_id='silver',
        bash_command="echo 'Running Silver'",
    )

    bronze >> silver

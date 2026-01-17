from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="real_estate_pipeline",
    default_args=default_args,
    schedule_interval=None,   
    catchup=False,
    tags=["real-estate", "spark", "elasticsearch"],
) as dag:

    spark_job = BashOperator(
        task_id="spark_transform",
        bash_command="""
        docker compose exec -T spark \
        /opt/spark/bin/spark-submit \
        /opt/project/scripts/combine_dvf_insee_spark.py
        """
    )

    load_to_es = BashOperator(
        task_id="load_to_elasticsearch",
        bash_command="""
        docker compose exec -T spark \
        python /opt/project/scripts/load_to_es.py
        """
    )

    spark_job >> load_to_es

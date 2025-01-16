
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="spark_test_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_test_task = SparkSubmitOperator(
        task_id="spark_test_with_connexion",
        application="/hello/jbo.py",  # Replace with your Spark job path
        conn_id="spark_default",  # Use the connection ID you created
    )

    spark_test_task


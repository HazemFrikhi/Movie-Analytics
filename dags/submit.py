from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    "first_test",
    default_args={"start_date": datetime(2023, 1, 1)},
    schedule_interval=None,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="spark_submit_job",
        application="/hello/jbo.py",
        conn_id="spark_default",
        verbose=True,
      
    )

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='spark_submit_example',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/path/to/your/spark-job.py',  # Local or HDFS path to your Spark script
        conn_id='spark_default',  # Connection ID created earlier
        application_args=['arg1', 'arg2'],  # Optional arguments for your Spark job
        conf={
            'spark.executor.memory': '2g',
            'spark.executor.cores': 2,
        },
        driver_memory='1g',
        verbose=True,  # Enable verbose logging
    )

    spark_task

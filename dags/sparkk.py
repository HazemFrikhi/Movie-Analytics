from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'spark_airflow_example',
    default_args=default_args,
    description='A simple Spark job example using Airflow',
    schedule_interval=None,  # Set your own schedule here
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # PySpark code as part of the DAG (within the operator)
    spark_submit_task = SparkSubmitOperator(
        task_id='submit_spark_job',
        conn_id='spark_default',  # Connection ID to Spark cluster (configured in Airflow UI)
        application="/opt/spark/bin/spark-submit",  # Path to the Spark submit binary (usually inside the Spark container)
        application_args=["--conf", "spark.ui.port=4040"],  # Add more arguments as needed
        conf={
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
        },
        name="airflow-spark-job",
        verbose=True,
        deploy_mode="client",  # or "cluster" if you're using a cluster
    )

    spark_submit_task

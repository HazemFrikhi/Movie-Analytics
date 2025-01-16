from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    "spark_random_dataframe_dag",  # DAG Name
    default_args=default_args,
    description="Run Spark transformations using SparkSubmitOperator",
    schedule_interval=None,  # No schedule for now, trigger manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task to submit the Spark job
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_random_dataframe_job",
        application="/spark_jobs/random_dataframe_job.py",  # Path to the Spark script
        conn_id="spark_default",  # Connection ID configured in Airflow
        application_args=[],
        conf={"spark.executor.memory": "1g", "spark.driver.memory": "1g"},
        executor_cores=2,
        name="random_dataframe_job",
        verbose=True,
    )

    submit_spark_job


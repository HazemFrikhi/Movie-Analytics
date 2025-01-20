from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import tempfile
import os

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Function that runs the Spark job (embedded in the same file)
def run_spark_job():
    from pyspark.sql import SparkSession

    # Start Spark session
    spark = SparkSession.builder.appName("EmbeddedSparkJob").getOrCreate()

    # Example Spark job logic
    data = [("Alice", 29), ("Bob", 31), ("Cathy", 25)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    df.show()

    # Write to output (adjust path based on your cluster configuration)
    

    # Stop Spark session
    spark.stop()

# Instantiate the DAG
with DAG(
    dag_id="embedded_spark_submit_function_example",
    default_args=default_args,
    description="A DAG with embedded Spark job function using SparkSubmitOperator",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Create a temporary Python file for the Spark job
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
        temp_file.write("""
from pyspark.sql import SparkSession

def run_spark_job():
    # Start Spark session
    spark = SparkSession.builder.appName("EmbeddedSparkJob").getOrCreate()

    # Example Spark job logic
    data = [("Alice", 29), ("Bob", 31), ("Cathy", 25)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    df.show()

    # Write to output (adjust path based on your cluster configuration)
    

    # Stop Spark session
    spark.stop()

run_spark_job()
""")
        temp_file_path = temp_file.name

    # Use SparkSubmitOperator to run the temporary file
    run_spark_task = SparkSubmitOperator(
        task_id="run_embedded_spark_job",
        conn_id="spark_default",  # Airflow connection ID for Spark
        name="embedded_spark_job",
        application=temp_file_path,  # Path to the temporary script
        verbose=True,
    )

    # Define the task execution order
    run_spark_task

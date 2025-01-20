from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random


def run_spark_job():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RandomDataTransformation") \
        .getOrCreate()

    # Generate random data
    data = [(random.randint(1, 100), random.choice(['A', 'B', 'C', 'D'])) for _ in range(100)]
    columns = ["id", "category"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Show the original DataFrame
    print("Original DataFrame:")
    df.show()

    # Transformation: Add a new column with a calculated value
    df_transformed = df.withColumn("new_value", col("id") * 2)

    # Transformation: Filter data based on a condition
    df_filtered = df_transformed.filter(col("id") > 50)

    # Show the transformed DataFrame
    print("Transformed DataFrame:")
    df_filtered.show()

    # Stop the Spark session
    spark.stop()


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="spark_dag_python_operator",
    default_args=default_args,
    description="A DAG that runs a Spark job using the PythonOperator",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['without_saving'],
) as dag:

    # Task to run the Spark job
    spark_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    # Define the DAG structure
    spark_task

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import random

# Define the function to run the Spark job
def run_spark_job():
    """
    Function to generate and process data using Spark
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("GenerateAndProcessData").getOrCreate()

    # Generate random data (1000 rows with two columns)
    data = [(random.randint(1, 100), random.random()) for _ in range(1000)]

    # Create a Spark DataFrame
    df = spark.createDataFrame(data, ["ID", "Value"])

    # Process data: Filter values greater than 0.5 and count occurrences by ID
    processed_df = df.filter(df["Value"] > 0.5).groupBy("ID").count()
    df.show()

    # Save the processed data to a CSV file
    processed_df.write.csv("/path/to/output/processed_data.csv", header=True)

    # Stop the Spark session
    spark.stop()

# Define the DAG
dag = DAG(
    'spark_hellojob',  # DAG name
    description='Run Spark job to generate and process data',
    schedule_interval=None,  # Run manually, or set a cron expression to schedule it
    start_date=datetime(2025, 1, 20),  # Start date of the DAG
    catchup=False,  # Don't backfill past runs
    tags=['hello_spark'],  # Tag for the DAG
)

# PythonOperator to execute the Spark job
spark_task = PythonOperator(
    task_id='run_spark_job',  # Task ID
    python_callable=run_spark_job,  # Function to call
    dag=dag,
)

# You can add more tasks here if needed

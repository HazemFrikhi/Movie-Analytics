from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
from datetime import datetime

# Function to define and run the Spark job with random data
def run_spark_job():
    # Initialize the SparkSession
    spark = SparkSession.builder \
        .appName("this_is_spark") \
        .getOrCreate()

    # Generate random data with 100 rows and 3 columns
    num_rows = 100
    df = spark.range(num_rows) \
        .withColumn("random_value1", rand()) \
        .withColumn("random_value2", rand()) \
        .withColumn("random_value3", rand())

    # Perform some transformations: selecting columns and creating new ones
    transformed_df = df.select("id", "random_value1", "random_value2", "random_value3") \
        .withColumn("sum_of_values", col("random_value1") + col("random_value2") + col("random_value3"))

    # Show the transformed DataFrame (for demonstration purposes)
    transformed_df.show()

    # Save the output to a new file (adjust the file format and location as needed)
    

    # Stop the Spark session when done
    spark.stop()

# Define the DAG and its schedule
dag = DAG(
    'spark_using_pythonOp',  # DAG name
    description='A simple Spark job DAG in Airflow with random data',
    schedule_interval=None,  # Runs once when triggered manually (can set to cron expression)
    start_date=datetime(2025, 1, 14),  # Start date for the DAG
    catchup=False,  # Don't backfill
)

# Define the Airflow task to run the Spark job
spark_task = PythonOperator(
    task_id='run_spark_task',
    python_callable=run_spark_job,  # Call the Spark job function
    dag=dag,
)

# Set up the DAG task dependencies (if any)
spark_task

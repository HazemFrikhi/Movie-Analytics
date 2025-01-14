from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG and its schedule
dag = DAG(
    'this_is_spark',  # DAG name
    description='A simple Spark job DAG in Airflow with random data using SparkSubmitOperator',
    schedule_interval=None,  # Runs once when triggered manually (can set to cron expression)
    start_date=datetime(2025, 1, 14),  # Start date for the DAG
    catchup=False,  # Don't backfill
)

# Spark job code (the spark job will be executed using a Python file)
# Here we write a Python file to execute the Spark job logic

spark_python_file = '/tmp/spark_job.py'

# Create a Python file containing the Spark job logic
spark_job_code = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# Initialize the Spark session
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

# Save the output to a new file (adjust the file format and location as needed)
transformed_df.write.csv("/tmp/spark_output.csv", header=True, mode="overwrite")

# Stop the Spark session when done
spark.stop()
"""

# Save the spark job code to the Python file
with open(spark_python_file, 'w') as f:
    f.write(spark_job_code)

# Define the SparkSubmitOperator task
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    conn_id='spark_default',  # Spark connection id defined in Airflow
    application=spark_python_file,  # Path to the Python job file
    dag=dag,
)

# Set up the task dependencies (if any)
spark_submit_task

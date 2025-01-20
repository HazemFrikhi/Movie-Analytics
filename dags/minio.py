from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

# Boto3 MinIO Configuration
MINIO_HOST = "http://minio:9000"  # Change 'minio' if different in your setup


def connect_to_minio():
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_HOST,
        aws_access_key_id="admin",
        aws_secret_access_key="admin123",
        region_name="us-east-1"
    )
    # Test the connection by listing the buckets
    try:
        response = s3_client.list_buckets()
        print("Connected to MinIO successfully!")
        print("Buckets:", response['Buckets'])
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}")

def test_minio_connection():
    connect_to_minio()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'minio_connection_dag',
    default_args=default_args,
    description='Test MinIO connection with boto3',
    schedule_interval=None,  # Only run manually or based on your needs
    start_date=datetime(2025, 1, 20),
    catchup=False,
)

# Task to test the connection
test_connection_task = PythonOperator(
    task_id='test_minio_connection',
    python_callable=test_minio_connection,
    dag=dag,
)

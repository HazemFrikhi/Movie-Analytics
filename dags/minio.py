from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

# Function to test the connection to MinIO
def test_minio_connection():
    # Create a hook with MinIO connection details (for testing)
    hook = S3Hook(
        aws_conn_id='minio_default',  # Connection ID (we'll set up these params manually)
        region_name='us-east-1',  # Can be any region (not necessary for MinIO, just for compatibility)
        host='http://<minio-release-name>.<namespace>.svc.cluster.local:9000',  # MinIO host URL
        aws_access_key_id='admin',  # Access key
        aws_secret_access_key='admin123',  # Secret key
    )

    bucket_name = 'test-bucket'
    
    # Check if the bucket exists
    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name=bucket_name)
    print(f"Bucket '{bucket_name}' exists or was created successfully!")

# Define the DAG
with DAG(
    dag_id="test_minio",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # No schedule, run on trigger
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id="test_minio_connection",
        python_callable=test_minio_connection,
    )

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Python functions for the tasks
def start_task():
    print("Starting the workflow...")

def extract_data():
    print("Extracting data...")

def transform_data():
    print("Transforming data...")

def load_data():
    print("Loading data into the target system...")

def end_task():
    print("Workflow completed!")

# Create the DAG
dag = DAG(
    'random_airflow_dag',
    default_args=default_args,
    description='A random example Airflow DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Define the tasks
start = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

end = PythonOperator(
    task_id='end_task',
    python_callable=end_task,
    dag=dag,
)

# Define the task dependencies
start >> extract >> transform >> load >> end
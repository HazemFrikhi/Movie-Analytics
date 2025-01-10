from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a function to perform a calculation
def calculate_sum_and_product(**kwargs):
    num1 = kwargs.get('num1', 5)
    num2 = kwargs.get('num2', 10)
    sum_result = num1 + num2
    product_result = num1 * num2
    print(f"The sum of {num1} and {num2} is {sum_result}")
    print(f"The product of {num1} and {num2} is {product_result}")
    return {"sum": sum_result, "product": product_result}

# Define the DAG
with DAG(
    'calculation_dag',
    default_args={'owner': 'airflow'},
    description='A simple DAG to test basic calculations',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task: Perform the calculation
    calculate_task = PythonOperator(
        task_id='calculate_sum_and_product',
        python_callable=calculate_sum_and_product,
        op_kwargs={"num1": 7, "num2": 3},
    )

calculate_task

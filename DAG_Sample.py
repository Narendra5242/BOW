from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],  # Specify the email ID here
    'email_on_failure': True,
    'email_on_retry': False,
}

# Python function to run as a task
def my_python_function():
    # Your Python code logic here
    print("Running my Python function")
    # Simulating some task completion
    return "Task completed successfully"

with DAG('email_example', default_args=default_args, schedule_interval='@once') as dag:
    
    # Task running Python function
    run_python_task = PythonOperator(
        task_id='run_python_task',
        python_callable=my_python_function
    )
    
    # Email notification task
    send_email = EmailOperator(
        task_id='send_email',
        to='recipient@example.com',  # Email ID of the recipient
        subject='Airflow Task Completion',
        html_content='<p>The Python task completed successfully.</p>',
    )
    
    # Define the task sequence
    run_python_task >> send_email  # The email task triggers after the Python task completion

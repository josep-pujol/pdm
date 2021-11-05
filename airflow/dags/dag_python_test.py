from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}


with DAG(
    'dag_python_test',
    default_args=default_args,
    description='Test Python DAG in Docker container',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    def print_context(context):
        print(context)
        return "context printed"

    t1 = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        op_kwargs={"context": "sadfasdfasfdasdf"},
    )

    def short_task(seconds):
        print("Starting short task... ", end="")
        time.sleep(seconds)
        print("short task completed")

    t2 = PythonOperator(
        task_id='short_task',
        python_callable=short_task,
        op_kwargs={"seconds": 5},
    )

    def long_task(seconds):
        print("Starting long task... ", end="")
        time.sleep(seconds)
        print("long task completed")

    t3 = PythonOperator(
        task_id='long_task',
        python_callable=long_task,
        op_kwargs={"seconds": 10},
    )

    t1 >> [t2, t3]

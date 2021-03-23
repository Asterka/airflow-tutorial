try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute():
    print("Hello World!")

with DAG(
    dag_id="my_first_dag.py",
    schedule_interval="@hourly",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay":timedelta(minutes=10),
        "start_date":datetime(2021,3,23)
    },
    catchup=False
) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute)
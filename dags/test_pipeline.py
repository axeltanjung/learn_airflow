import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def read_file():
    data = pd.read_csv("/opt/airflow/dags/data_source/North America Restaurants.csv")
    return data

def transform_data(ti):
    data = ti.xcom_pull(task_id="extract")
    data_filtered = dat[data["state"] == "AB"]

    return data_filtered

def save_file(ti):
    data = ti.xcom_pull(task_id="transfrom")

    data.to_csv("/opt/airflow/dags/output/transform_data.csv", index=False)


with DAG(
    dag_id = "test_pipeline",
    start_date = datetime.datetime(2024, 2, 14),
    schedule_interval = "5 7 * * *",
) as dag:
    start = DummyOperator(
        task_id = "start",
        dag = dag
    )

    extract = PythonOperator(
        task_id = "exctract",
        python_callable=read_file,
        dag = dag
    )

    load = PythonOperator(
        task_id = "transform",
        python_callable=transform_data,
        dag = dag
    )

    transform = PythonOperator(
        task_id = "load",
        python_callable=save_file,
        dag = dag
    )

    end = DummyOperator(
        task_id = "end",
        dag = dag
    )

    start >> extract >> load >> transform >> end
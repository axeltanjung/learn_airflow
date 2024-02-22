import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.providers.operators.postgres import PostgresOperator

# initiating Postgres Operator

with DAG(
    dag_id = "postgres_operator_dag",
    start_date = datetime.datetime(2020, 2, 2),
    scheduler_interval = "@once"
    catchup=False
) as dag:
    create_pet_table =PostgresOperator(
        task_id = "create_pet_table",
        postgres_conn_id = "postgres_default",
        sql = '/opt/airflow/dags/sql/pet/list_pet.sql'
    )
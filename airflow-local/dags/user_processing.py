from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2021,3,22)
}

with DAG(
    dag_id = 'postgres_operator_dag',
    schedule_interval = '@daily', 
    default_args = default_args, 
    catchup=False
    ) as dag:
        creating_table = PostgresOperator(
            task_id='creating_table',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE TABLE IF NOT EXISTS users(
                    firstname   TEXT NOT NULL,
                    lastname    TEXT NOT NULL,
                    country     TEXT NOT NULL,
                    username    TEXT NOT NULL,
                    password    TEXT NOT NULL,
                    email       TEXT NOT NULL PRIMARY KEY
                );
                '''
        )


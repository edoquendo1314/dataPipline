from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


from datetime import datetime
import json

default_args = {
    'start_date': datetime(2021,3,22)
}

with DAG(
    dag_id = 'postgres_operator_dag',
    schedule_interval = '@daily', 
    default_args = default_args, 
    catchup=False
    ) as dag:
        creating_table=PostgresOperator(
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

        is_api_available=HttpSensor(
            task_id='is_api_available',
            http_conn_id='user_api',
            endpoint='api/'
        )

        extracting_user=SimpleHttpOperator(
            task_id='extracting_user',
            http_conn_id='user_api',
            endpoint='api/',
            method='GET',
            response_filter=lambda response: json.loads(response.text),
            log_response=True
        )


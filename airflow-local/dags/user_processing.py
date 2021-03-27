from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from pandas import json_normalize
import json

default_args = {
    'start_date': datetime(2021,3,22)
}

def _processing_user(ti):
    users = ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    processed_user = {
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    }
    value_list = ["'" + x + "'" for x in processed_user.values()]
    return ",".join(value_list);


with DAG(
    dag_id = 'postgres_operator_dag',
    schedule_interval = '@hourly', 
    default_args = default_args, 
    catchup=False,
    ) as dag:
        creating_table=PostgresOperator(
            task_id='creating_table',
            postgres_conn_id='postgres_default_2',
            sql='''
                CREATE TABLE IF NOT EXISTS users(
                    firstname   TEXT NOT NULL,
                    lastname    TEXT NOT NULL,
                    country     TEXT NOT NULL,
                    username    TEXT NOT NULL,
                    password    TEXT NOT NULL,
                    email       TEXT NOT NULL PRIMARY KEY
                );
                ''',
        )

        storing_user = PostgresOperator(
          task_id="storing_user",
          postgres_conn_id="postgres_default_2",
          sql="INSERT INTO users (firstname, lastname, country, username, password, email) VALUES ({{ task_instance.xcom_pull(task_ids='processing_user') }})"

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

        processing_user = PythonOperator(
            task_id='processing_user',
            python_callable=_processing_user
        )

        creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user

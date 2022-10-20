import json
import time
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def send_line_notify():
    url = 'https://notify-api.line.me/api/notify'
    token = '1aDXOIcl3z8MB1jHvbDjBNcmeNQCjWlVfplfckgJj5n'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    msg = "todos report today \n"

    r = requests.post(url, headers=headers, data={'message': msg})
    print(r.text)


with DAG(
    dag_id='sales_analytic_dag',
    schedule_interval='01 00 * * *',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    pass

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )

    task_save = BashOperator(
        task_id='save',
        bash_command='date'
    )

    task_sent_line_notify = PythonOperator(
        task_id='send_line_notify',
        python_callable=send_line_notify
    )

    task_end = BashOperator(
        task_id='end',
        bash_command='date'
    )

task_start >> task_save >> task_sent_line_notify >> task_end

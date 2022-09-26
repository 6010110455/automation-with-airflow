from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago


def callable_virtualenv_collect_joke():
    import requests
    import json
    from csv import DictWriter
    resp = requests.get('https://official-joke-api.appspot.com/random_joke')
    if resp.status_code != 200:
        return f'Failed with response code {resp.status_code}'
    else:
        joke_dict = json.loads(resp.text)
        with open('jokes.csv', 'a+', newline='') as write_obj:
            field_names = ['id', 'type', 'setup', 'punchline']
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            dict_writer.writerow(joke_dict)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'joke_collector',
    default_args=default_args,
    description='A DAG to collect a joke from a random joke generator API',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['automationwithairflow'],
) as dag:

    joke_collector_task = PythonVirtualenvOperator(
        task_id="joke_collector_task",
        python_callable=callable_virtualenv_collect_joke,
        requirements=["requests==2.25.1"],
        system_site_packages=True,
    )
    joke_collector_task

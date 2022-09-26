import json
import time
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def save_users_json(ti) -> None:
    users = ti.xcom_pull(task_ids=['get_users'])

    print(users[0])

    # Create json to /data
    json_object_users = json.dumps(users[0], indent=4)
    with open("C:\\Users\\60101\\Desktop\\Data_en\\airflow\\automation-with-airflow\\data\\users.json", "w") as outfile:
        outfile.write(json_object_users)


def save_posts_json(ti) -> None:
    posts = ti.xcom_pull(task_ids=['get_posts'])

    print(posts[0])

    # Create json to /data
    json_object_posts = json.dumps(posts[0], indent=4)
    with open("C:\\Users\\60101\\Desktop\\Data_en\\airflow\\automation-with-airflow\\data\\posts.json", "w") as outfile:
        outfile.write(json_object_posts)


def save_comments_json(ti) -> None:
    comments = ti.xcom_pull(task_ids=['get_comments'])

    print(comments[0])

    # Create json to /data
    json_object_comment = json.dumps(comments[0], indent=4)
    with open("C:\\Users\\60101\\Desktop\\Data_en\\airflow\\automation-with-airflow\\data\\comments.json", "w") as outfile:
        outfile.write(json_object_comment)


def save_todos_json(ti) -> None:
    todos = ti.xcom_pull(task_ids=['get_todos'])

    print(todos[0])

    # Create json to /data
    json_object_todos = json.dumps(todos[0], indent=4)
    with open("C:\\Users\\60101\\Desktop\\Data_en\\airflow\\automation-with-airflow\\data\\todos.json", "w") as outfile:
        outfile.write(json_object_todos)


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
    dag_id='parallel_dag',
    schedule_interval='@hourly',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    pass

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )

    task_is_api_active = HttpSensor(
        task_id="is_api_active",
        http_conn_id="api_posts",
        endpoint="posts/"
    )

    task_get_users = SimpleHttpOperator(
        task_id='get_users',
        http_conn_id="api_posts",
        endpoint="users/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_get_posts = SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id="api_posts",
        endpoint="posts/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_get_comments = SimpleHttpOperator(
        task_id='get_comments',
        http_conn_id="api_posts",
        endpoint="comments/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_get_todos = SimpleHttpOperator(
        task_id='get_todos',
        http_conn_id="api_posts",
        endpoint="todos/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_save = BashOperator(
        task_id='save',
        bash_command='date'
    )

    task_save_users = PythonOperator(
        task_id='save_users_json',
        python_callable=save_users_json
    )

    task_save_posts = PythonOperator(
        task_id='save_posts_json',
        python_callable=save_posts_json
    )

    task_save_comments = PythonOperator(
        task_id='save_comments_json',
        python_callable=save_comments_json
    )

    task_save_todos = PythonOperator(
        task_id='save_todos_json',
        python_callable=save_todos_json
    )

    task_sent_line_notify = PythonOperator(
        task_id='send_line_notify',
        python_callable=send_line_notify
    )

    task_end = BashOperator(
        task_id='end',
        bash_command='date'
    )

task_start >> task_is_api_active >> [task_get_users, task_get_posts, task_get_comments, task_get_todos] >> task_save >> [
    task_save_users, task_save_posts, task_save_comments, task_save_todos] >> task_sent_line_notify >> task_end

from email.mime import message
import json
import time
import requests
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import os
import pandas as pd
import datetime

from pandas.io.json import json_normalize
from pymongo import MongoClient


API_URL = "https://stp.eappsoft.net/api/v1"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjYxMzcwYzMwMWMyNzdjMDdlOGI5ZjgyYSIsInVzZXJuYW1lIjoic3VwZXJ1c2VyIiwiZXhwIjoxNjcwNDYzNzc0LCJpYXQiOjE2NjI2ODc3NzR9.EqCp-cHJeMmxtKIUA-7fvWlbmCIAxzYS-wmCMzhzufg"
size = 6000


def send_line_notify_start():
    url = 'https://notify-api.line.me/api/notify'
    token = '1aDXOIcl3z8MB1jHvbDjBNcmeNQCjWlVfplfckgJj5n'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    msg = "เริ่มการทำงาน ETL \n"
    r = requests.post(url, headers=headers, data={'message': msg})

    print(r.text)


def sent_line_notify_end():
    url = 'https://notify-api.line.me/api/notify'
    token = '1aDXOIcl3z8MB1jHvbDjBNcmeNQCjWlVfplfckgJj5n'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    msg = "จบการทำงาน ETL \n"
    r = requests.post(url, headers=headers, data={'message': msg})

    print(r.text)


def send_line_notify_message_year(message):
    url = 'https://notify-api.line.me/api/notify'
    token = '1aDXOIcl3z8MB1jHvbDjBNcmeNQCjWlVfplfckgJj5n'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    msg = message
    r = requests.post(url, headers=headers, data={'message': msg})

    print(r.text)


def get_data_from_api():
    query_payload = {
        'size': 6000,
        'page': 1,
        # 'startDate': "2022-09-30",
        # 'endDate': "2022-10-31",
        'type': "ออก",
        'DashBoardPage': 'true'
    }

    sales_transaction = requests.request(method='GET',
                                         url=f"{API_URL}/product-transaction",
                                         params=query_payload,
                                         headers={
                                                'Content-Type': 'application/json', 'Authorization': f'Bearer {TOKEN}'}
                                         )

    sales_transaction_json = json.loads(sales_transaction.content)
    sales_transaction_rows = sales_transaction_json['rows']
    st_df = pd.DataFrame(sales_transaction_rows)

    st_df['createdAt'] = pd.to_datetime(st_df['createdAt'])
    st_df['month'] = (st_df['createdAt']).dt.month
    st_df['year'] = (st_df['createdAt']).dt.year

    st_df['Time'] = pd.to_datetime(st_df['createdAt'])
    st_gy_df = st_df.groupby(
        ['year'])['total_price_offline_out_before'].sum().reset_index()

    st_gy_df = st_gy_df.sort_values(by=['year']).reset_index(drop=True)
    st_gy_df = st_gy_df.rename(
        columns={'total_price_offline_out_before': 'total_price'})

    this_year = datetime.date.today().strftime("%Y")
    this_year_sales = st_gy_df.loc[st_gy_df['year'] == this_year]

    this_year = datetime.date.today().strftime("%Y")
    this_year_sales = st_gy_df.loc[st_gy_df['year'].astype(
        "string") == this_year]

    this_year_sales['total_price']

    if this_year_sales.empty:
        this_year_sales.loc['0'] = ['0', '0']
    this_year_sales_str = this_year_sales["total_price"]
    this_year_sales.reset_index(drop=True, inplace=True)

    last_year = int(this_year) - 1
    last_year = str(last_year)

    last_year_sales = st_gy_df.loc[st_gy_df['year'].astype(
        "string") == last_year]

    last_year_sales['total_price']

    if last_year_sales.empty:
        last_year_sales.loc['0'] = ['0', '0']
    last_year_sales_str = last_year_sales["total_price"]
    last_year_sales.reset_index(drop=True, inplace=True)

    last_year_sales_str = round(last_year_sales_str, 2).astype("string")
    last_year_sales_str

    print("this_year", this_year)
    print("this_year_sales_str", this_year_sales_str)
    print("last_year_sales_str", last_year_sales_str)

    message = "สรุปประจำปี " + this_year + "\n▶รายได้ทั้งหมด" + " = " + this_year_sales_str + \
        " บาท" + "\n▶รายได้ทั้งหมดปีก่อน" + " = " + last_year_sales_str + " บาท"

    print("message", message)

    send_line_notify_message_year(message)


with DAG(
    dag_id='sales_year_dag',
    schedule_interval='0 0 1 * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    pass

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )

    # task_get_data_from_api1 = PythonOperator(
    #     task_id='get_data_from_api1',
    #     python_callable=get_data_from_api
    # )

    task_get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=send_line_notify_start
    )

    task_end = BashOperator(
        task_id='end',
        bash_command='date'
    )

task_start >> task_get_data_from_api >> task_end

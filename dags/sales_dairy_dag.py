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


def send_line_notify_message_dairy(message):
    url = 'https://notify-api.line.me/api/notify'
    token = '1aDXOIcl3z8MB1jHvbDjBNcmeNQCjWlVfplfckgJj5n'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    msg = message
    r = requests.post(url, headers=headers, data={
                      'message': msg, 'stickerPackageId': 1070, 'stickerId': 17854})

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
    st_df['day'] = (st_df['createdAt']).dt.day
    st_df['month'] = (st_df['createdAt']).dt.month
    st_df['year'] = (st_df['createdAt']).dt.year

    st_df['Time'] = pd.to_datetime(st_df['createdAt'])

    st_gmy_df = st_df.groupby(['year', 'month', 'day'])[
        'total_price_offline_out_before'].sum().reset_index()

    st_gmy_df = st_gmy_df.rename(
        columns={'total_price_offline_out_before': 'total_price'})

    st_gmy_df = st_gmy_df.assign(Date=st_gmy_df.year.astype(
        str) + '-' + st_gmy_df.month.astype(str) + '-' + st_gmy_df.day.astype(str))
    st_gmy_df.drop(['month', 'year', 'day'], axis=1, inplace=True)

    today = datetime.date.today().strftime("%Y-%m-%d").replace('-0', '-')
    today_sales = st_gmy_df.loc[st_gmy_df['Date'] == today]
    if today_sales.empty:
        today_sales.loc['0'] = ['0', '0']
    today_sales_str = today_sales["total_price"].astype("string")

    yesterdayDate = datetime.datetime.now() - datetime.timedelta(1)
    yesterday = yesterdayDate.strftime("%Y-%m-%d").replace('-0', '-')
    yesterday_sales = st_gmy_df.loc[st_gmy_df['Date'] == yesterday]
    if yesterday_sales.empty:
        yesterday_sales.loc['0'] = ['0', '0', '0', '0', '0']
    yesterday_sales_str = yesterday_sales["total_price"].astype("string")

    print("today", today)
    print("today_sales_str", today_sales_str.reset_index(drop=True, inplace=True))
    print("yesterday_sales_str", yesterday_sales_str.reset_index(
        drop=True, inplace=True))

    message = "สรุปประจำวัน \n" + "วันที่ " + today + \
        "\n▶รายได้ทั้งหมด" + " = " + today_sales_str + " บาท" + \
        "\n▶รายได้ทั้งหมดเมื่อวาน" + " = " + yesterday_sales_str + " บาท"
    print("message", message)

    send_line_notify_message_dairy(message)


with DAG(
    dag_id='sales_daily_dag',
    schedule_interval='01 00 * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    pass

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )

    task_send_line_notify_start = PythonOperator(
        task_id='send_line_notify_start',
        python_callable=send_line_notify_start
    )

    task_get_data_from_api = PythonOperator(
        task_id='send_get_data_from_api',
        python_callable=get_data_from_api
    )

    task_sent_line_notify_end = PythonOperator(
        task_id='sent_line_notify_end',
        python_callable=sent_line_notify_end
    )

    task_end = BashOperator(
        task_id='end',
        bash_command='date'
    )

task_start >> task_send_line_notify_start >> task_get_data_from_api >> task_sent_line_notify_end >> task_end

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


def send_line_notify_message_month(message):
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
    st_df['month'] = (st_df['createdAt']).dt.month
    st_df['year'] = (st_df['createdAt']).dt.year

    st_df['Time'] = pd.to_datetime(st_df['createdAt'])
    st_gmy_df = st_df.groupby(['month', 'year'])[
        'total_price_offline_out_before'].sum().reset_index()
    st_gmy_df = st_gmy_df.sort_values(
        by=['year', 'month']).reset_index(drop=True)
    st_gmy_df_my = st_gmy_df.rename(
        columns={'total_price_offline_out_before': 'total_price'})

    st_gmy_df_my = st_gmy_df_my.assign(Date=st_gmy_df_my.year.astype(
        str) + '-' + st_gmy_df_my.month.astype(str))
    st_gmy_df_my.drop(['month', 'year'], axis=1, inplace=True)

    this_month = datetime.date.today().strftime("%Y-%m").replace(' 0', ' ')
    this_month_sales = st_gmy_df_my.loc[st_gmy_df_my['Date'] == this_month]

    print("this_month_sales", this_month_sales)

    if this_month_sales.empty:
        this_month_sales.loc['0'] = ['0', '0']
    this_month_sales_str = this_month_sales["total_price"]
    this_month_sales_str = round(this_month_sales_str, 2).astype("string")
    this_month_sales_str.reset_index(drop=True, inplace=True)

    today1 = datetime.date.today()
    first1 = today1.replace(day=1)
    last_month = (first1 - datetime.timedelta(days=1)
                  ).strftime("%Y-%m").replace('-0', '-')
    last_month_sales = st_gmy_df_my.loc[st_gmy_df_my['Date'] == last_month]

    if last_month_sales.empty:
        last_month_sales.loc['0'] = ['0', '0']
    last_month_sales_str = last_month_sales["total_price"]
    last_month_sales_str = round(last_month_sales_str, 2).astype("string")
    last_month_sales_str.reset_index(drop=True, inplace=True)

    print("this_month", this_month)
    print("this_month_sales_str", this_month_sales_str)
    print("last_month_sales_str", last_month_sales_str)

    message = "สรุปประจำเดือน " + this_month + "\n▶รายได้ทั้งหมด" + " = " + this_month_sales_str + \
        " บาท" + "\n▶รายได้ทั้งหมดเดือนก่อน" + " = " + last_month_sales_str + " บาท"

    print("message", message)

    send_line_notify_message_month(message)


with DAG(
    dag_id='sales_month_dag',
    schedule_interval='0 0 * * 0',
    start_date=days_ago(1),
    catchup=False
) as dag:
    pass

    task_start = BashOperator(
        task_id='start',
        bash_command='date'
    )

    # task_send_line_notify_start = PythonOperator(
    #     task_id='send_line_notify_start',
    #     python_callable=send_line_notify_start
    # )

    task_get_data_from_api = PythonOperator(
        task_id='send_get_data_from_api',
        python_callable=get_data_from_api
    )

    # task_sent_line_notify_end = PythonOperator(
    #     task_id='sent_line_notify_end',
    #     python_callable=sent_line_notify_end
    # )

    task_end = BashOperator(
        task_id='end',
        bash_command='date'
    )

task_start >> task_get_data_from_api >> task_end

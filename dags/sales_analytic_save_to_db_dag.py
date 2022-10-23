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
import mysql.connector
from sqlalchemy import create_engine, DateTime

from pandas.io.json import json_normalize
from pymongo import MongoClient


API_URL = "https://stp.eappsoft.net/api/v1"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjYxMzcwYzMwMWMyNzdjMDdlOGI5ZjgyYSIsInVzZXJuYW1lIjoic3VwZXJ1c2VyIiwiZXhwIjoxNjcwNDYzNzc0LCJpYXQiOjE2NjI2ODc3NzR9.EqCp-cHJeMmxtKIUA-7fvWlbmCIAxzYS-wmCMzhzufg"
size = 6000

HOST_URL = 'localhost'
MYSQL_PASSWORD = '29012542'
MYSQL_USER = 'localadmin'
MYSQL_DATABASE = 'sales_storedb'
MYSQL_TABLE_NAME = 'salesstore1'


def send_line_notify_start():
    url = 'https://notify-api.line.me/api/notify'
    token = '1aDXOIcl3z8MB1jHvbDjBNcmeNQCjWlVfplfckgJj5n'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    msg = "เริ่มดึงข้อมูล เพื่อบันทึกลงดาต้าเบส\n"
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

    msg = "จบการทำงาน บันทึกข้อมูลสำเร็จ\n"
    r = requests.post(url, headers=headers, data={'message': msg})

    print(r.text)


def get_data_from_api_and_insert_to_db():
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

    # Spreading Customer Object  into Customer Name
    customer_sales_df = pd.DataFrame(st_df['customer'].to_dict())

    customer_sales_df = customer_sales_df.transpose()
    customer_type_sales_df = pd.DataFrame(customer_sales_df['type'].to_dict())
    customer_type_sales_df = customer_type_sales_df.transpose()
    # Combine into main datafram
    customer_sales_df['customer_type_name'] = customer_type_sales_df['name']
    customer_sales_df['type'] = customer_type_sales_df['_id']

    # Combine into main datafram
    st_df['customer_name'] = customer_sales_df['name']
    st_df['customer_type'] = customer_sales_df['customer_type_name']
    st_df['customer'] = customer_sales_df['_id']

    # explode order
    st_df_explode = st_df.explode('order')
    st_df_explode = st_df_explode.reset_index()

    # Spreading Order Object  into Product Name
    product_sales_df = pd.DataFrame(st_df_explode['order'].to_dict())
    product_sales_df = product_sales_df.transpose()

    product_sales_df_product = pd.DataFrame(
        product_sales_df['product'].to_dict())
    product_sales_df_product = product_sales_df_product.transpose()

    product_sales_df['product_name'] = product_sales_df_product['name']
    product_sales_df['product_type_code'] = product_sales_df_product['type_code']
    product_sales_df['product_price'] = product_sales_df_product['price']
    product_sales_df['product_cost_price'] = product_sales_df_product['cost_price']
    product_sales_df['product_inventory'] = product_sales_df_product['inventory']
    product_sales_df['product_unit'] = product_sales_df_product['unit']
    product_sales_df['product'] = product_sales_df_product['_id']

    st_df_explode['product_name'] = product_sales_df['product_name']
    st_df_explode['product_type_code'] = product_sales_df['product_type_code']
    st_df_explode['product_price'] = product_sales_df['product_price']
    st_df_explode['product_cost_price'] = product_sales_df['product_cost_price']
    st_df_explode['product_inventory'] = product_sales_df['product_inventory']
    st_df_explode['product_unit'] = product_sales_df['product_unit']
    st_df_explode['order'] = product_sales_df['product']
    st_df_explode['amount'] = product_sales_df['amount']
    st_df_explode['price_per_amount'] = product_sales_df['price']

    st_df_explode.drop(['__v', 'quotation'], axis=1, inplace=True)
    employee_df = pd.DataFrame(st_df_explode['employee'].to_dict())
    employee_df = employee_df.transpose()
    employee_df_department = pd.DataFrame(employee_df['department'].to_dict())
    employee_df_department = employee_df_department.transpose()
    employee_df['department_name'] = employee_df_department['name']
    employee_df['department_code'] = employee_df_department['department_code']
    st_df_explode['employee_firstname'] = employee_df['firstname']
    st_df_explode['employee_lastname'] = employee_df['lastname']
    st_df_explode['employee_department_name'] = employee_df['department_name']
    st_df_explode['employee_department_code'] = employee_df['department_code']
    st_df_explode['date'] = st_df_explode['createdAt']

    st_df_explode.drop(['employee', 'product_transaction_type', 'updatedAt', '_id', 'Time', 'day',
                       'month', 'year', 'remark', 'index', 'date', 'order', 'id'], axis=1, inplace=True)

    st_df_explode["employee_department_code"].fillna("-", inplace=True)
    st_df_explode["customer"].fillna("-", inplace=True)
    st_df_explode["sale_type"].fillna("-", inplace=True)
    st_df_explode["customer_name"].fillna("-", inplace=True)
    st_df_explode["customer_type"].fillna("-", inplace=True)
    st_df_explode['product_cost_price'].astype(float)

    # Connect to database
    db = mysql.connector.connect(
        host='mysql-mysql-1', port="3306", user='root', password='29012542', database='sales_store_db'
    )

    if not (db.is_connected):
        print('Fail to Connect')
    else:
        print('Connect Successfully')

    create_table_script = """
        CREATE TABLE IF NOT EXISTS salesstore2 (
            id INT(8) NOT NULL PRIMARY KEY AUTO_INCREMENT,
            customer VARCHAR(256),
            bill_no VARCHAR(256),
            sale_type VARCHAR(64),
            customer_name VARCHAR(256),
            customer_type VARCHAR(256),
            product_name VARCHAR(256),
            product_type_code VARCHAR(256),
            product_price FLOAT(26),
            product_cost_price FLOAT(26),
            product_inventory FLOAT(26),
            product_unit VARCHAR(256),
            amount FLOAT(24),
            price_per_amount FLOAT(26),
            createdAt TIMESTAMP,
            total_price_offline_out_before FLOAT(26),
            employee_firstname VARCHAR(128),
            employee_lastname VARCHAR(128),
            employee_department_name VARCHAR(128),
            employee_department_code VARCHAR(128)
        )
    """

    cursor = db.cursor()
    cursor.execute(create_table_script)
    print('Create Table Success')

    # Create SQL ALchemy Connector
    connection_string = f'mysql+mysqlconnector://root:29012542@mysql-mysql-1/sales_store_db'
    print('Connection String', connection_string)

    mysql_engine = create_engine(connection_string)

    datatype = {
        "createdAt": DateTime
    }

    st_df_explode.to_sql(con=mysql_engine, name='salesstore2',
                         if_exists='replace', chunksize=10, index=False, dtype=datatype)
    print("Success")


with DAG(
    dag_id='sales_analytic_save_to_db_dag',
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

    task_get_data_from_api_and_insert_to_db = PythonOperator(
        task_id='get_data_from_api_and_insert_to_db',
        python_callable=get_data_from_api_and_insert_to_db
    )

    task_sent_line_notify_end = PythonOperator(
        task_id='sent_line_notify_end',
        python_callable=sent_line_notify_end
    )

    task_end = BashOperator(
        task_id='end',
        bash_command='date'
    )

task_start >> task_send_line_notify_start >> task_get_data_from_api_and_insert_to_db >> task_sent_line_notify_end >> task_end

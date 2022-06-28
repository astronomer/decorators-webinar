from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime
import json
from typing import Dict


def _extract_data():
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    order_data_dict = json.loads(data_string)
    return order_data_dict

def _process_data(ti):
    order_data_dict = ti.xcom_pull(task_ids='extract_data')
    total_order_value = 0
    for value in order_data_dict.values():
        total_order_value += value

    ti.xcom_push(key='total_order_value', value=total_order_value)

def _store_data(ti):
    total_order_value = ti.xcom_pull(task_ids='process_data', key='total_order_value')
    print(f"Total order value is: {total_order_value:.2f}")

with DAG('classic_dag', schedule_interval='@daily', start_date=datetime(2021, 12, 1), catchup=False) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=_extract_data
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=_process_data
    )

    store_data = PythonOperator(
        task_id='store_data',
        python_callable=_store_data
    )

    extract_data >> process_data >> store_data
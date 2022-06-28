import json
from airflow.decorators import dag, task, task_group

import pendulum
from datetime import datetime, timedelta


data_strings = {
    'apple': '{"1001": 301.27, "1002": 433.21, "1003": 502.22}',
    'carrot': '{"1001": 305.62, "1002": 425.71, "1003": 504.18}', 
    'banana': '{"1001": 310.14, "1002": 431.98, "1003": 503.55}',
    'orange': '{"1001": 310.14, "1002": 431.98, "1003": 503.55}'
}


@dag(schedule_interval=None, start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False)

def task_group_dynamic_example():

    @task_group(group_id='extracts')
    def extract_tasks():
        for data_string in data_strings:
            @task(task_id='extract_{0}'.format(data_string))
            def extract_data(data_string):
                data = data_strings[data_string]
                order_data_dict = json.loads(data)
                return order_data_dict

            extract_data(data_string)

    @task()
    def transform():
        print("Data has been transformed")

    extract_tasks() >> transform()
    
task_group_dynamic_example = task_group_dynamic_example()

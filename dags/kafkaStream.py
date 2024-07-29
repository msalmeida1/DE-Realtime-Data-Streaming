import uuid
from datetime import datetime
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import time
import logging

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'matheus',
    'start_date': datetime(2024, 7, 28, 10, 00)
}

# random user API
def get_data() -> Dict[str, Any]:
    response = requests.get("https://randomuser.me/api/")
    response.raise_for_status()
    data = response.json()['results'][0]
    return data

# format data
def format_data(response: Dict[str, Any]) -> Dict[str, Any]:
    data = {}
    location = response['location']
    data['id'] = str(uuid.uuid4())
    data['firstName'] = response['name']['first']
    data['lastName'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postCode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registeredDate'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data

# stream
def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    start_time = time.time()

    while time.time() <= start_time + 60:
        try:
            response = get_data()
            formatted_data = format_data(response)
            producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            time.sleep(1)

with DAG('user_automation', default_args=default_args, schedule='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(task_id='stream_data_from_api', python_callable=stream_data)

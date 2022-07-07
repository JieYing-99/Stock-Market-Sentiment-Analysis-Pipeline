from pydruid.client import *
import requests
import json
import time
from datetime import datetime

from common import DRUID_OVERLORD_SOCKET, DRUID_BROKER_SOCKET, convert_timestamp_to_ISO8601_format, get_timestamp_days_before, get_current_eastern_timestamp, convert_time_to_local_time, TIMESTAMP_FORMAT, get_scraped_count, TWITTER_DATA_SOURCE_NAME, NEWS_MARKET_DATA_SOURCE_NAME, NEWS_STOCK_DATA_SOURCE_NAME

DRUID_SUPERVISOR_ENDPOINT = f'http://{DRUID_OVERLORD_SOCKET}/druid/indexer/v1/supervisor'
DRUID_TASKS_ENDPOINT = f'http://{DRUID_OVERLORD_SOCKET}/druid/indexer/v1/tasks'

DRUID_INGESTION_SPEC_PATH = 'input/druid_ingestion_specs/{}.json'

def submit_supervisor_spec(data_source):
    with open(DRUID_INGESTION_SPEC_PATH.format(data_source)) as json_file:
        json_data = json.load(json_file)
    response = requests.post(DRUID_SUPERVISOR_ENDPOINT, json=json_data).json()
    print(response)

def get_supervisor_ids():
    response = requests.get(DRUID_SUPERVISOR_ENDPOINT).json()
    print(response)

def get_supervisor_status(data_source):
    response = requests.get(f'{DRUID_SUPERVISOR_ENDPOINT}/{data_source}/status').json()
    is_suspended = response['payload']['suspended']
    detailed_state = response['payload']['detailedState']
    print(f'suspended: {is_suspended}, state: {detailed_state}')
    return detailed_state

def suspend_supervisor(data_source):
    response = requests.post(f'{DRUID_SUPERVISOR_ENDPOINT}/{data_source}/suspend').json()
    # print(response)
    while (get_supervisor_status(data_source) != 'SUSPENDED'):
      time.sleep(1)

def resume_supervisor(data_source):
    response = requests.post(f'{DRUID_SUPERVISOR_ENDPOINT}/{data_source}/resume').json()
    # print(response)
    while (get_supervisor_status(data_source) != 'RUNNING'):
      time.sleep(1)

def get_running_task_id(data_source):
    while True:
      response = requests.get(f'{DRUID_TASKS_ENDPOINT}', params={'datasource': data_source, 'state': 'running'}).json()
      if response:
        break
      time.sleep(1)
    task_id = response[0]['id']
    print(f"task id: {task_id}")
    return task_id

def get_task_status(data_source, task_id):
    scraped_count = get_scraped_count(data_source)
    while True:
      response = requests.get(f'{DRUID_SUPERVISOR_ENDPOINT}/{data_source}/stats').json()
      if response:
        n_records_processed = response['0'][task_id]['totals']['buildSegments']['processed']
        break
      time.sleep(1)
    print(f'{n_records_processed} out of {scraped_count} records processed')
    return n_records_processed >= scraped_count

def get_latest_timestamp(data_source):
    query = PyDruid(f'http://{DRUID_BROKER_SOCKET}', 'druid/v2')
    result = query.scan(
        datasource=data_source,
        granularity='all',
        intervals=f'{convert_timestamp_to_ISO8601_format(get_timestamp_days_before(timestamp=get_current_eastern_timestamp(), n_days=1))}/p1d',
        columns=['date']
    )
    latest_timestamp = result.export_pandas().sort_values(by=['date'], ascending=False).iloc[0]['date']
    duration_from_now = datetime.strptime(get_current_eastern_timestamp().strftime(TIMESTAMP_FORMAT), TIMESTAMP_FORMAT) - datetime.strptime(latest_timestamp, TIMESTAMP_FORMAT)
    days = duration_from_now.days
    hours, remainder = divmod(duration_from_now.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f'latest timestamp: {latest_timestamp} ET ({convert_time_to_local_time(latest_timestamp)}) --> {days} days {hours} hours {minutes} minutes from now')

if __name__ == '__main__':
    data_sources = [TWITTER_DATA_SOURCE_NAME, NEWS_MARKET_DATA_SOURCE_NAME, NEWS_STOCK_DATA_SOURCE_NAME]
    for data_source in data_sources:
        print(f'\n --{data_source}--')
        resume_supervisor(data_source)
        task_id = get_running_task_id(data_source)
        while (not get_task_status(data_source, task_id)):
            time.sleep(3)
        get_latest_timestamp(data_source)
        suspend_supervisor(data_source)
        time.sleep(5)
import gzip
import requests
import json
import os
import re
import psycopg2.extras
from base64 import b64decode
from datetime import datetime, timezone


# Database connection parameters
DB_HOST = os.environ['DB_HOST']
DB_PORT = '5432' 
DB_USER = 'tsuser'
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = 'tsdb' 

GITHUB_TOKEN = os.environ['GITHUB_TOKEN']

# Set up the headers with the authorization token
headers = {
    'Authorization': f'token {GITHUB_TOKEN}'
}

log_pattern = re.compile(r'received: (.*})')

def unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

def handler():
    # Connect to the TimescaleDB

    print("Connecting to db host" + DB_HOST)
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cursor = conn.cursor()

    repos = ['ComposeDbExampleApp','lit-composedb','ceramic-eas','ceramic-ai','verifiable-credentials']
    for repo in repos:

        response = requests.get(
    'https://api.github.com/repos/ceramicstudio/{}/traffic/clones'.format(repo),
    headers=headers
        )       
        data = response.json()
        for clone_data in data.get('clones', {}):
           day_str = datetime.strptime(clone_data['timestamp'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")

           q = "insert into developer_repo_clones (timestamp, unique_count, repo_name) values ('{}', {}, '{}') on conflict do nothing".format(day_str, clone_data['uniques'], repo)
           cursor.execute(q)           


    conn.commit()
    cursor.close()
    conn.close()

    print("Done, check the db")

handler()

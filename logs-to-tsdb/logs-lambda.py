import gzip
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

log_pattern = re.compile(r'received: (.*})')

def unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

def handler(event, context):
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

    # Decompress and decode the log data
    compressed_payload = b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    payload = json.loads(uncompressed_payload)

    batched = []

    for ev in payload['logEvents']:
        # Transform log_event as required and add to batched_logs

        ts = unix_to_datetime(ev['timestamp']/1000)
    
        match = log_pattern.search(ev.get('message',''))
        if not match:
            continue

        data = json.loads(match.group(1))

        batched.append((ts, data['cid'], data['did'], data.get('model'), data.get('family'), data.get('stream'), data.get('origin'), data.get('cacao'), data.get('cap_cid')))
    
    # Insert logs into TimescaleDB
    insert_query = "INSERT INTO cas_log_data (timestamp, cid, did, model, family, stream, origin, cacao, cap_cid) VALUES %s"

    print("Inserting {} rows including {}".format(len(batched), batched[0]))

    psycopg2.extras.execute_values(cursor, insert_query, batched)

    conn.commit()
    cursor.close()
    conn.close()

    print("Done, check the db")

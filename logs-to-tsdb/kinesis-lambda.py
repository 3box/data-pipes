import gzip
import json
import os
import re
import psycopg2.extras
from base64 import b64decode, b64encode
from datetime import datetime, timezone
from pprint import pprint, pformat
from warnings import warn

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
    pprint(event)
    warn(pformat(event))
    # Connect to the TimescaleDB
    print("Connecting to db host " + DB_HOST)
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cursor = conn.cursor()

    batched = []

    print("NOT decoding data")
    for record in event['Records']:

        payload = record['kinesis']['data']
        # it doesn't appear to be encoded
        #  payload = b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        for ev in data['logEvents']:
            # Transform log_event as required and add to batched_logs
            ts = unix_to_datetime(ev['timestamp'] / 1000)

            match = log_pattern.search(ev.get('message', ''))
            if not match:
                continue

            log_data = json.loads(match.group(1))
            batched.append((ts, log_data['cid'], log_data['did'], log_data.get('model'), log_data.get('family'), log_data.get('stream'), log_data.get('origin'), log_data.get('cacao'), log_data.get('cap_cid')))

    # Insert logs into TimescaleDB
    insert_query = "INSERT INTO cas_log_data (timestamp, cid, did, model, family, stream, origin, cacao, cap_cid) VALUES %s"

    if batched:
        print("Inserting {} rows including {}".format(len(batched), batched[0]))
        psycopg2.extras.execute_values(cursor, insert_query, batched)
        conn.commit()
    else:
        print("No valid log data to insert")

    cursor.close()
    conn.close()

    print("Kinesis pipe done, check the db")

# For testing locally
if __name__ == "__main__":

    # Mock event and context for testing locally
    encoded_event = {
        "Records": [
            {
                "kinesis": {
                    "data": b64encode(json.dumps({
                        "logEvents": [
                            {
                                "timestamp": 1625239073000,
                                "message": "your log message"
                            }
                        ]
                    }).encode('utf-8')).decode('utf-8')
                }
            }
        ]
    }

    event = {
        "Records": [
            {
                "kinesis": {
                    "data": json.dumps({
                        "logEvents": [
                            {
                                "timestamp": 1625239073000,
                                "message": "your log message"
                            }
                        ]
                    })
                }
            }
        ]
    } 
    pprint(event)
    context = {}

    handler(event, context)

import gzip
import json
import os
import re
import psycopg2
from base64 import b64decode

# Database connection parameters
#DB_HOST = os.environ['DB_HOST']
#DB_PORT = os.environ['DB_PORT']
#DB_USER = os.environ['DB_USER']
#DB_PASSWORD = os.environ['DB_PASSWORD']
#DB_NAME = os.environ['DB_NAME']

log_pattern = re.compile(r'received: (.*})')

def handler(event, context):
    # Connect to the TimescaleDB
#    conn = psycopg2.connect(
#        host=DB_HOST,
#        port=DB_PORT,
#        user=DB_USER,
#        password=DB_PASSWORD,
#        dbname=DB_NAME
#    )
#    cursor = conn.cursor()


    # Decompress and decode the log data
    compressed_payload = b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    payload = json.loads(uncompressed_payload)


    batched = []

    for ev in payload['logEvents']:
        # Transform log_event as required and add to batched_logs

        import sys; sys.stdin = open('/dev/tty')
        import pdb; pdb.set_trace()
        match = log_pattern.search(ev.get('message',''))
        if match:
            data = json.loads(match.group(1))
 
        # Example: batched_logs.append((log_event['id'], log_event['message']))
        pass
    
    # Insert logs into TimescaleDB
    insert_query = "INSERT INTO your_table_name (column1, column2) VALUES %s"
    psycopg2.extras.execute_values(cursor, insert_query, batched_logs)

    conn.commit()
    cursor.close()
    conn.close()


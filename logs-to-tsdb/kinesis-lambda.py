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
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = '5432'
DB_USER = 'tsuser'
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = 'tsdb'

log_pattern = re.compile(r'received: (.*})')

def unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

def handler(event, context):
    # Connect to the TimescaleDB
    if DB_HOST:
      # normally we should connect, in tests we do not 
      # print("Connecting to db host " + DB_HOST + " database " + DB_NAME)
      conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
      )
      cursor = conn.cursor()

    batched = []

    for record in event['Records']:
        payload = b64decode(record['kinesis']['data'])
        decoded = gzip.decompress(payload).decode('utf-8')
        data = json.loads(decoded)

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
        print("Inserting {} rows".format(len(batched)))
#        print("Inserting {} rows including {}".format(len(batched), batched[0]))
        psycopg2.extras.execute_values(cursor, insert_query, batched)
        conn.commit()
    else:
        print("No valid log data to insert")

    cursor.close()
    conn.close()

#    print("Kinesis pipe done, check the db")

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
     "Records": [{"awsRegion": "us-east-2",
    "eventID": "shardId-000000000000:49652274695115981779048041884426019726251995147880366082",
    "eventName": "aws:kinesis:record",
    "eventSource": "aws:kinesis",
    "eventSourceARN": "arn:aws:kinesis:us-east-2:967314784947:stream/ceramic-cas-log-stream-tnet",
    "eventVersion": "1.0",
    "invokeIdentityArn": "arn:aws:iam::967314784947:role/lambda_role",
    "kinesis": {"approximateArrivalTimestamp": 1716361597.516,
    "data": "H4sIAAAAAAAA/02RS2/TQBSF/0p0N2ySdF6e1y5AWqgoFBIBoqkqezyNHb9n/Ihd9b+jGipY3cV39B3p3CcorPfh0e7H2oKG95v95uFmu9ttrrawhGoorQMNiguKmZBMMQFLyKvjlau6GjRcWOMvjHVhkZpVW9p2ZUL/J7JrnQ0L0GBC/xDW6cXrpVLwQDIWGoMZU0QFCGMuGIllRA0JYAm+i7xxad2mVXmZ5q11HvQdvNvsPlVHv/uPwv1ctu1t2b5kniCNQQOVRHDMCEeIIUQFkpxRpBBRhFFEXzqZkpjTgCLGEBWcyiAQDJbQpoX1bVjUoLHAnHIcqAAJtXzdCjTcEUTYCgUrQvZIaMQ1DdZIqF/3i483t1++7Tef93rxZlOapHILZ5vO+nbhrLFpb2O9eDqASeMD6ANE4dE0LwvmPSOUFIzRjA95NyXT2CcTTWlGeZaU5+EcNFkcN1FS2V7UtTg24QGWB4j/muI01pkd9cRvsur7Nipu2dur6+v+NPKTuZx+up2NfObaIYvZron6D9XX9IcMh1lSVLHNZ012mnKe9I8uGriRZ+aQTKg6UdTmNMTqjJxEKQ6SLhpk+Nizx44MqlDVMDETVfls8/Pv/+kyMxZikKMQZdbldWPxyffEB8l5SoJiwmPSFg1JxpOfrJD2VCpzLs6DNcdZV7n0mJazjrK1wGtMyRorNEMTmrCa2QGe38Dz/fNv+/wUvtYCAAA=",
    "kinesisSchemaVersion": "1.0",
    "partitionKey": "e4883c0bfd788e81bb5d969a4fcd7d49",
    "sequenceNumber": "49652274695115981779048041884426019726251995147880366082"}}]
    } 
    pprint(event)
    context = {}

    handler(event, context)

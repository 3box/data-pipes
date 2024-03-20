import gzip
import requests
import json
import os
import re
import psycopg2.extras
from base64 import b64decode
from datetime import datetime, timezone, timedelta


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

def fetch_data(query, cur):
      cur.execute(query)
      return cur.fetchall()

def compare_and_update(last_week_data, prev_week_data, week, cur):
    # Convert list of tuples to dictionary for easy lookup
    last_week_dict = dict(last_week_data)
    prev_week_dict = dict(prev_week_data)

    # Find new significant sources
    new_significant = [(source, week, count) for source, count in last_week_dict.items() if source not in prev_week_dict]

    # Find missing significant sources
    missing_significant = [(source, week, prev_week_dict[source], last_week_dict.get(source, 0)) for source in prev_week_dict if source not in last_week_dict]

    return new_significant, missing_significant

def update_tables(new_significant, missing_significant, cur):
      # Create or replace new_significant table
      cur.execute("DROP TABLE IF EXISTS new_significant;")
      cur.execute("CREATE TABLE new_significant (source text, week_ending DATE, unique_write_count bigint);")
      psycopg2.extras.execute_batch(cur, "INSERT INTO new_significant (source, week_ending, unique_write_count) VALUES (%s, %s, %s);", new_significant)

      # Create or replace missing_significant table
      cur.execute("DROP TABLE IF EXISTS missing_significant;")
      cur.execute("CREATE TABLE missing_significant (source text, week_ending DATE, previous_count bigint, new_count bigint);")
      psycopg2.extras.execute_batch(cur, "INSERT INTO missing_significant (source, week_ending, previous_count, new_count) VALUES (%s, %s, %s, %s);", missing_significant)


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
    cur = conn.cursor()

    # Queries to execute
    query_last_week = """
    SELECT source, unique_write_count
    FROM top_sources_7d
    WHERE unique_write_count > 1000
    AND week = date_trunc('week', current_date) - interval '1 week';
    """

    query_prev_week = """
    SELECT source, unique_write_count
    FROM top_sources_7d
    WHERE unique_write_count > 1000
    AND week = date_trunc('week', current_date) - interval '2 weeks';
    """

    last_week_data = fetch_data(query_last_week, cur)
    prev_week_data = fetch_data(query_prev_week, cur)
    week = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime('%Y-%m-%d')

    new_significant, missing_significant = compare_and_update(last_week_data, prev_week_data, week, cur)

    update_tables(new_significant, missing_significant, cur)

    conn.commit()
    cur.close()
    conn.close()

    print("Done, check the db")

handler()

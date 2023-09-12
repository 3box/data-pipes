import psycopg2
import re
import os
import subprocess
import requests

DATABASE_URL = os.environ.get('TS_DATABASE_URL')

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


def dag_get(cid):
    url = f'http://localhost:5001/api/v0/dag/get?arg={cid}'
    response = requests.post(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def decode_protected_sig(sig_value):
    cmd = ['node', 'cmd-decode-sig.mjs']
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(input=sig_value.encode())
    
    # Check for errors
    if process.returncode != 0:
        print(f"Error executing node script: {stderr.decode()}")
        return None

    return stdout.decode()


def get_cacao_from_ipfs(cid, cap_cid=None):
    # Create an IPFS client connection
    if not cap_cid:
        try:
            data = dag_get(cid)
            psig = data["signatures"][0]["protected"]
            psig_data = decode_protected_sig(psig)            
            cap_cid = re.sub('ipfs://', '', psigdata.get('cap'))
         
        except Exception as e:
            print("Error: " + str(e))
            return (None, None)


    try:
        cap_data = dag_get(cap_cid)
    except Exception as e:
        print("Error: " + str(e))
        return (cap_cid, None)


    try:
        cacao = cap_data['p']['domain']
        return (cap_cid, cacao)

    except Exception as e:
        print("Error: " + str(e))
        return (cap_cid, None) 

    client.close()


################################################################################################
# Update the failure count in the database
#
def handle_failure(cid):
    """
    Handles failures for a specific CID. If the CID doesn't have an entry in the failure table,
    it's added. Otherwise, the count is incremented.
    """
    # Increment the failure count for the CID
    cur.execute("UPDATE cas_log_data SET cacao_failures = COALESCE(cacao_failures, 0) + 1 WHERE cid = %s;", (cid,))


################################################################################################
#  Get the rows from the main database that we haven't got cacaos for yet
#
def get_rows_with_null_cacao():

    # Get the latest last_updated timestamp from cid_cacao table
    cur.execute("SELECT MAX(last_updated) FROM cid_cacao;")
    latest_last_updated = cur.fetchone()[0]

    # Get rows we need to process
    if latest_last_updated:
        cur.execute("""
SELECT cas_log_data.cid, cas_log_data.cap_cid
FROM cas_log_data
LEFT JOIN cid_cacao ON cas_log_data.cid = cid_cacao.cid
WHERE cid_cacao.cid IS NULL AND cas_log_data.timestamp_column_name > %s;
        """, (latest_last_updated,))
    else:
        cur.execute("""
SELECT cas_log_data.cid, cas_log_data.cap_cid
FROM cas_log_data
LEFT JOIN cid_cacao ON cas_log_data.cid = cid_cacao.cid
WHERE cid_cacao.cid IS NULL
LIMIT 10;
     """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


################################################################################################
# Main loop - process all the rows, in batches, to retrieve and record the cacaos and cap cids
#
def process_rows(rows, batch_size=100):
    successful_updates = []
    failed_updates = []

    for row in rows:
        cid, cap_cid = row
        (cap, cacao) = get_cacao_from_ipfs(cid, cap_cid) 
        if cacao:
            successful_updates.append((cid, cap, cacao))
        else:
            failed_updates.append(cid)

        # If batch size reached, process
        if len(successful_updates) >= batch_size:
            execute_batch_updates(cur, successful_updates, failed_updates)
            successful_updates.clear()
            failed_updates.clear()

    # Process any remaining records
    if successful_updates or failed_updates:
        execute_batch_updates(cur, successful_updates, failed_updates)

    conn.commit()
    cur.close()
    conn.close()


################################################################################################
# this just batches the dadtabase updates
#
def execute_batch_updates(cur, successful_updates, failed_updates):
    # Insert or Update cid_cacao with cacao data
    upsert_query = """
        INSERT INTO cid_cacao (cid, cap, cacao) VALUES %s
        ON CONFLICT (cid) DO UPDATE SET 
        cap = EXCLUDED.cap, 
        cacao = EXCLUDED.cacao,
        last_updated = now();
    """
    psycopg2.extras.execute_values(cur, upsert_query, successful_updates)

    # Batch increment failures
    for cid in failed_updates:
        cur.execute("""
            INSERT INTO cid_cacao (cid, failure_count) VALUES (%s, 1)
            ON CONFLICT (cid) DO UPDATE SET 
            failure_count = COALESCE(cid_cacao.failure_count, 0) + 1,
            last_updated = now();
        """, (cid,))


def run_batch():
    rows = get_rows_with_null_cacao()
    process_rows(rows)


if __name__ == "__main__":
    run_batch()

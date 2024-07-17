import subprocess
from datetime import datetime, timedelta, timezone
import time

def start_query_for_period(log_group, start_date, end_date):
    query = "fields @timestamp, @message, @logStream, @log | filter @message LIKE 'Anchor request received'"
    
    cmd = [
        "aws", "logs", "start-query",
        "--log-group-name", log_group,
        "--start-time", str(int(start_date.timestamp() * 1000)),  # in milliseconds
        "--end-time", str(int(end_date.timestamp() * 1000)),  # in milliseconds
        "--query-string", query,
        "--query", "queryId", 
        "--output", "text"
    ]
    try:
      query_id = subprocess.check_output(cmd).decode().strip()
      return query_id
    except Exception as e:
      print("Error starting query: " + str(e))
      print("Command was:")
      print(" ".join(cmd))
      print("----------------")
      return 

def get_query_results(query_id, filename):
    cmd = [
        "aws", "logs", "get-query-results",
        "--query-id", query_id,
        ">", filename
    ]

    subprocess.call(" ".join(cmd), shell=True)

def main():
    log_group = "/ecs/ceramic-prod-cas"
    
    start_date_str = "2024-07-01 23:59:00"
    end_date_str = "2024-07-15 20:00:00"
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    
    query_ids = []

    # Start all the queries
    while start_date < end_date:
        next_date = start_date + timedelta(minutes=5)
        query_id = start_query_for_period(log_group, start_date, next_date)
        if query_id:
            query_ids.append((query_id, start_date.strftime('%Y-%m-%d-%H-%M')))  # Storing the date for filename
        else:
            print("SKIPPING {} to {}".format(start_date.strftime('%Y-%m-%d-%H-%M'), end_date.strftime('%Y-%m-%d-%H-%M')))
        
        start_date = next_date

    # After starting all the queries, let's wait 20 seconds
    time.sleep(20)

    # Fetch the results for each query and save to a file
    for query_id, date in query_ids:
        get_query_results(query_id, f"results_{date}.txt")

if __name__ == "__main__":
    main()


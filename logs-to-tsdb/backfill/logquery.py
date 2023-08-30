from time import (time, sleep)
import os
"""
aws logs start-query \
  --log-group-name "/ecs/ceramic-prod-cas" \
  --start-time $(date -d "2023-06-01 00:00:00" +%s)000 \
  --end-time $(date -d "2023-08-29 16:00:00" +%s)000 \
  --query-string "fields @timestamp, @message, @logStream, @log | filter @message LIKE 'Anchor request received'"
"""


# rate approx 1000/hr
# get 3 hr increments to be on safe side

qstart = epoch_start
while qstart < epoch_now:
   end_time = str(qstart + qwindow)
   start_time = str(qstart)

   cmd = "aws logs start-query --region us-east-2 --log-group-names \"{}\" --start-time {} --end-time {} --limit {} --query-string \"{}\" | grep 'queryId' >> latest-queries.txt".format(log_group_names, start_time, end_time, limit, query)
   os.system(cmd) 

   sleep(10)
   qstart += qwindow

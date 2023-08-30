import re
import os
import json
import subprocess

"""
[
    {
        "@timestamp": "2023-08-30 06:03:16.578",
        "@message": "[2023-08-30T06:03:16.578Z] INFO: 'Anchor request received: {\"cid\":\"bagcqceravmb2qbmha4wr2lbjoj6i7pbg6z2sd3ls6w4fm4yycxwsq2bsqzba\",\"did\":\"did:pkh:eip155:1:0xc5fceb38c67c8e3b0cd509cee320c868ed971e3a\",\"family\":\"IDX\",\"stream\":\"k2t6wyfsu4pfyrn8uhelr7zxkqvuy3q4f061c12iq2w394c5wxmz9hcjhc5q7b\",\"origin\":\"did:key:z6MkmY9VdxnrHLSvYw9Ja7iBF44x2JUiUNvAYcmaV5gatEx3\"}'",
        "@logStream": "cas_api/cas_api/1e9ab363f8d540af9fd9b19e676deeba",
        "@log": "967314784947:/ecs/ceramic-prod-cas"
    },
"""

LOGSTR = { "messageType": "DATA_MESSAGE",
  "owner": "123456789012",
  "logGroup": "/ecs/ceramic-prod-cas",
   "logStream": "",
   "logEvents": []
}

ls = {}
cnt = 0

with open('backfill_data.json', 'r') as f:
    logs = json.load(f)
    for ev in logs:
       cnt += 1
       if cnt>2:
          break
       logstr = ev['@logStream']
       if logstr not in ls:
         ls[logstr] = LOGSTR.copy()
         ls[logstr]['logStream'] = logstr
       nicev = {
         "logStreamName": ev['@logStream'],
         "timestamp": ev['@timestamp'],
         "message": ev['@message']
       }
       ls[logstr]['logEvents'].append(nicev)


LAM_TEMP = { "awslogs": { "data": "" }}

for logs in ls:
    (grp,part,logid) = logs.split('/')
    fname = 'logs_{}.json'.format(logid)
    with open(fname, 'w') as f:
        json.dump(ls[logs], f)
    os.system('gzip {}'.format(fname))
    newl = LAM_TEMP.copy()
    newl['awslogs']['data'] = subprocess.run(['base64', '{}.gz'.format(fname)], capture_output=True, text=True).stdout.strip()

    with open('ready_{}.json'.format(logid), 'w') as f:
       json.dump(newl, f)

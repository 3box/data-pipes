import re
import os
import sys
import json
import subprocess
from datetime import datetime

"""
{
    "results": [
        [
            {
                "field": "@timestamp",
                "value": "2023-08-02 06:59:48.506"
            },
            {
                "field": "@message",
                "value": "[2023-08-02T06:59:48.506Z] INFO: 'Anchor request received: {\"cid\":\"bagcqceraj7wqu5ggc5kvhdtkjafugeicnpqlvh42yntwwi3ko6mzuycr7oaq\",\"did\":\"did:3:kjzl6cwe1jw149gjuvqxc4k4lfc3xiaaqu42u9v5nkj9z5it35i12gz824iavel\",\"family\":\"disco-public-user-data\",\"stream\":\"k2t6wyfsu4pfyb8i1dyazkpsw2w16o7bkt5p4xsazvrrzr5zcipy4bp9otynnz\",\"origin\":\"did:key:z6MktDG68nTy2YvkgvRFpRQ2PiodWJQh5hNwr7zefmgwqvBq\"}'"
            },
            {
                "field": "@logStream",
                "value": "cas_api/cas_api/cd0c53d9a7f44cc9887a5916e996e9ae"
            },
            {
                "field": "@log",
                "value": "967314784947:/ecs/ceramic-prod-cas"
            },
            {
                "field": "@ptr",
                "value": "CmcKJgoiOTY3MzE0Nzg0OTQ3Oi9lY3MvY2VyYW1pYy1wcm9kLWNhcxAEEjkaGAIGS2So7wAAAAWWnvFVAAZMn+tgAAAG8iABKIHeq6ibMTDx4rOomzE4sRJAmssqSLLiBFDU3gQYACABEI4EGAE="
            }
        ],
"""

LOGSTR = { "messageType": "DATA_MESSAGE",
  "owner": "123456789012",
  "logGroup": "/ecs/ceramic-prod-cas",
   "logStream": "",
   "logEvents": []
}

ls = {}
cnt = 0

backfile = sys.argv[1]

with open(backfile, 'r') as f:
    logs = json.load(f)
    for res in logs['results']:
       ev = {}
       for pair in res:
         if pair['field'] == '@logStream':
           logstr = pair['value']
         if pair['field'] == '@message':
           msg = pair['value']
         if pair['field'] == '@timestamp':
           dt = datetime.strptime(pair['value'], "%Y-%m-%d %H:%M:%S.%f")
           tm = int(dt.timestamp()) * 1000
       if logstr not in ls:
         ls[logstr] = LOGSTR.copy()
         ls[logstr]['logStream'] = logstr
       nicev = {
         "logStreamName": logstr,
         "timestamp": tm,
         "message": msg 
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
    cmd = 'python-lambda-local -f handler ../logs-lambda.py ready_{}.json --timeout 20'.format(logid)
    os.system('python-lambda-local -f handler ../logs-lambda.py ready_{}.json --timeout 20'.format(logid)) 

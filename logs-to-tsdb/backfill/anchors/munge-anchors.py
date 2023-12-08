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
                "value": "2023-11-08 09:43:07.657"
            },
            {
                "field": "@message",
                "value": "[2023-11-08T09:43:07.656Z] INFO: 'Created anchor commit with CID bafyreiajie4ozkhqzicwygaruxgsit3k4c7grnkppnwx3x55olbb6dofsi for stream k2t6wyfsu4pfyqbahubx5p3ztoxk9phoymxso9ly3rc0bcqk32zxo85v1wor1a'"
            },
            {
                "field": "@logStream",
                "value": "cas_anchor/cas_anchor/acfbd097255044a2a8c0de094bc07aea"
            },
            {
                "field": "@log",
                "value": "967314784947:/ecs/ceramic-prod-cas"
            },
            {
                "field": "@ptr",
                "value": "CmcKJgoiOTY3MzE0Nzg0OTQ3Oi9lY3MvY2VyYW1pYy1wcm9kLWNhcxABEjkaGAIGU2iuegAAAAA44TaoAAZUtX8QAAAAoiABKKbxvvK6MTCV1cTyujE45BJA+6QsSPm4BFCbtQQYACABENYHGAE="
            }
        ],
"""

backfile = sys.argv[1]

PATT = re.compile(r'Created anchor commit with CID (\S+) for stream ([^\']+)')

with open(backfile, 'r') as f:
    logs = json.load(f)
    for res in logs['results']:
       cid = ''
       stream = ''
       dtstr = ''
       for pair in res:
         if pair['field'] == '@message':
           msg = pair['value']
           match = PATT.search(msg)
           if match: 
               cid, stream = match.groups()
         if pair['field'] == '@timestamp':
           dtstr = pair['value']
       print("{},{},{}".format(dtstr, stream, cid))


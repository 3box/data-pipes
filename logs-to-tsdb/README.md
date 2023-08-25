# logs-to-tsdb

Lambda function for pulling CAS Cloudwatch logs into TimescaleDB

Designed for running automatically, but may be tested locally
as follows.

## Prerequisites for Local Development

First, retrieve some data

```
aws logs filter-log-events --log-group-name "/ecs/ceramic-prod-cas" --filter-pattern "Anchor request received" --start-time 1692743008000 --end-time 1692743608000 --limit 100
```

collect some data from the same stream into a json file with headers like sample.json

```
gzip -c sample.json
base64 sample.json.gz > sample_base64.txt
```
Then insert that text into the provided file `sample_test.json`

We also need to set up the local environment

```
virtualenv venv
. venv/bin/activate
pip3 install -r requirements.txt
pip3 install python-lambda-local
```

## Running locally

```
. venv/bin/activate
python-lambda-local -f handler logs-lambda.py sample_test.json -t 600
```

New images are [published here](https://us-east-2.console.aws.amazon.com/ecr/repositories/private/967314784947/data-pipes-logs?region=us-east-2)



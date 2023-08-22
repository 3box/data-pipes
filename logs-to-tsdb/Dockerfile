FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt ${LAMBDA_TASK_ROOT}

COPY logs-to-tsdb.py ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

CMD [ "logs-to-tsdb.handler" ]

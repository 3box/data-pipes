FROM public.ecr.aws/lambda/python:3.10

ENV LAMBDA_TASK_ROOT /var/task

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt ${LAMBDA_TASK_ROOT}

COPY kinesis-lambda.py ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

CMD [ "kinesis-lambda.handler" ]


FROM public.ecr.aws/lambda/python:3.10

ENV LAMBDA_TASK_ROOT /var/task

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt ${LAMBDA_TASK_ROOT}

COPY retrieve-cacaos.py ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

CMD [ "retrieve-cacaos.run_batch" ]

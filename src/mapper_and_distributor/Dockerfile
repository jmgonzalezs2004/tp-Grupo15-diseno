FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY mapper_and_distributor .

ENTRYPOINT ["python3", "main.py"]

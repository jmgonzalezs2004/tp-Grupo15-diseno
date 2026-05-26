FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY amount_filter .

ENTRYPOINT ["python3", "main.py"]

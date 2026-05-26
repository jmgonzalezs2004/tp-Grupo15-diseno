FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY three_chain .

ENTRYPOINT ["python3", "main.py"]

FROM python:3.12-slim

WORKDIR /app

COPY ./src/approach_b_message_broker/tasks.py .
COPY ./src/approach_b_message_broker/producer.py .
COPY ./src/approach_b_message_broker/celeryconfig.py .

RUN pip install pandas celery==5.3.6 "pika>=1.2.0"

RUN chmod -R 777 /app

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]

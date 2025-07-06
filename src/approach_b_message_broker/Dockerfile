FROM python:3.12-slim
WORKDIR /app
COPY src/approach_b_message_broker/ .
COPY data/ /app/data/
RUN pip install pandas celery
CMD ["python", "-m", "approach_b_message_broker.producer"]

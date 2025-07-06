FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install pandas celery numpy

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]

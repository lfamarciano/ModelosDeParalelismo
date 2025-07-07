FROM python:3.12-slim

WORKDIR /app

COPY ./src/approach_a_local_processing/process_local.py .

RUN pip install pandas 

RUN chmod -R 777 /app

CMD ["python", "process_local.py"]

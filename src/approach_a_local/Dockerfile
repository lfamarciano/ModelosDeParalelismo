FROM python:3.12-slim
WORKDIR /app
COPY src/approach_a_local/ .
COPY data/ /app/data/
RUN pip install pandas
CMD ["python", "process_local.py", "4"]

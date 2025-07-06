FROM python:3.12-slim
WORKDIR /app
COPY src/data_generator.py .
RUN pip install faker pandas
CMD ["python", "data_generator.py", "20", "50000", "0.02"]

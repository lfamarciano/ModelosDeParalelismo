# ./src/approach_data_generator/Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY ./src/data_generator/data_generator.py .

RUN pip install numpy pandas pathlib

CMD ["python", "data_generator.py"]

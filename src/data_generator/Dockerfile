# ./src/approach_data_generator/Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install numpy pandas pathlib

CMD ["python", "data_generator.py"]

FROM bitnami/spark:3.4

WORKDIR /app

COPY . .

# Dependências Python
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install pandas pyspark

USER 1001

CMD ["python3", "process_spark.py"]

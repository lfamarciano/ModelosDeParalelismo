FROM bitnami/spark:3.4

WORKDIR /app

COPY ./src/approach_c_spark_processing/process_spark.py .

USER root
RUN pip install pyspark==3.4.1 pandas
USER 1001

CMD ["python3", "process_spark.py"]

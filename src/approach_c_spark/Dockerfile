FROM bitnami/spark:latest
COPY src/approach_c_spark/ /app/
COPY data/ /app/data/
WORKDIR /app
CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "local[*]", "main.py"]

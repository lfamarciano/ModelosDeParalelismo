from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, countDistinct
import time
import json

THRESHOLDS = {
    "temperatura": (-10, 45),
    "umidade": (0, 100),
    "pressao": (940, 1060)
}

def main():
    start = time.perf_counter()

    spark = SparkSession.builder \
        .appName("MeteorologicalSparkProcessing") \
        .getOrCreate()

    df = spark.read.csv("/app/data/dados_meteorologicos.csv", header=True, inferSchema=True)
    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # Anomalias
    for sensor, (min_val, max_val) in THRESHOLDS.items():
        df = df.withColumn(f"{sensor}_anormal", ~((col(sensor) >= min_val) & (col(sensor) <= max_val)))

    # Métrica 1: Percentual de anomalias por estação e sensor
    anomalias = []
    for sensor in THRESHOLDS:
        total = df.groupBy("id_estacao").count().withColumnRenamed("count", "total")
        anom = df.filter(col(f"{sensor}_anormal")).groupBy("id_estacao").count().withColumnRenamed("count", "anom")
        joined = total.join(anom, "id_estacao", "left").fillna(0)
        final = joined.withColumn("sensor", col("id_estacao") * 0 + sensor)\
                      .withColumn("percentual_anomalias", (col("anom") / col("total")) * 100)\
                      .select("id_estacao", "sensor", "percentual_anomalias")
        anomalias.append(final)

    df_anomalias = anomalias[0]
    for extra in anomalias[1:]:
        df_anomalias = df_anomalias.union(extra)

    # df_anomalias.coalesce(1).write.csv("/app/output/percentuais_anomalias", header=True, mode="overwrite")

    # Métrica 2: Média móvel de 10min por região (sem anomalias)
    clean_df = df.filter(~col("temperatura_anormal") & ~col("umidade_anormal") & ~col("pressao_anormal"))

    moving_avg = clean_df.groupBy(
        window("timestamp", "10 minutes"),
        "regiao"
    ).agg(
        avg("temperatura").alias("media_temperatura"),
        avg("umidade").alias("media_umidade"),
        avg("pressao").alias("media_pressao")
    )

    # moving_avg.selectExpr(
    #     "window.start as inicio_janela", "window.end as fim_janela", "regiao",
    #     "media_temperatura", "media_umidade", "media_pressao"
    # ).coalesce(1).write.csv("/app/output/media_movel_regiao", header=True, mode="overwrite")

    # Métrica 3: Períodos com múltiplos sensores anômalos em 10min por estação
    multianomalias = df.withColumn("soma_anomalias",
        col("temperatura_anormal").cast("int") +
        col("umidade_anormal").cast("int") +
        col("pressao_anormal").cast("int")
    )

    cooc = multianomalias.filter(col("soma_anomalias") > 1)\
        .groupBy("id_estacao", window("timestamp", "10 minutes"))\
        .agg(countDistinct("timestamp").alias("eventos"))

    # cooc.groupBy("id_estacao").count()\
    #     .withColumnRenamed("count", "periodos_multianomalias_10min")\
    #     .coalesce(1).write.csv("/app/output/periodos_coocorrencia", header=True, mode="overwrite")

    end = time.perf_counter()
    duration_ms = (end - start) * 1000
    with open("data/tempo_execucao.json", "w") as f:
        json.dump({"tempo": duration_ms}, f)
    print(f"Spark finalizado em {end - start:.2f} segundos.")

if __name__ == "__main__":
    main()

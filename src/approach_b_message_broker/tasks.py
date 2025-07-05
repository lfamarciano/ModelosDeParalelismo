from celery import Celery
import pandas as pd
import numpy as np

app = Celery('tasks')
app.config_from_object('approach_b_message_broker.celeryconfig')

THRESHOLDS = {
    "temperatura": (-10, 45),
    "umidade": (0, 100),
    "pressao": (940, 1060)
}

def detectar_anomalias(df):
    for sensor in ["temperatura", "umidade", "pressao"]:
        min_val, max_val = THRESHOLDS[sensor]
        df[f"{sensor}_anormal"] = ~df[sensor].between(min_val, max_val)
    return df

@app.task
def calcular_percentual_anomalias(estacao_csv):
    df = pd.read_json(estacao_csv)
    id_est = str(df["id_estacao"].iloc[0])
    resultados = []
    for sensor in ["temperatura", "umidade", "pressao"]:
        total = len(df)
        anomalias = (~df[sensor].between(*THRESHOLDS[sensor])).sum()
        resultados.append({
            "id_estacao": id_est,
            "sensor": sensor,
            "percentual_anomalias": float(100 * anomalias / total)
        })
    return resultados

@app.task
def calcular_periodos_coocorrencia(estacao_csv):
    df = pd.read_json(estacao_csv)
    df = detectar_anomalias(df)
    df = df.sort_values("timestamp").set_index("timestamp")
    grupo = df[["temperatura_anormal", "umidade_anormal", "pressao_anormal"]]
    rolling = grupo.rolling("10min").sum()
    mask = (rolling > 0).sum(axis=1) > 1
    return {
        "id_estacao": str(df["id_estacao"].iloc[0]),
        "periodos_multianomalias_10min": int(mask.sum())
    }

@app.task
def calcular_media_movel(regiao_csv):
    df = pd.read_json(regiao_csv)
    df = detectar_anomalias(df)
    df = df.sort_values("timestamp").set_index("timestamp")
    df = df[~(df["temperatura_anormal"] | df["umidade_anormal"] | df["pressao_anormal"])]
    rolling = df[["temperatura", "umidade", "pressao"]].rolling("10min").mean()
    rolling["regiao"] = df["regiao"].iloc[0]
    rolling["timestamp"] = rolling.index
    return rolling.reset_index(drop=True).to_dict(orient="records")

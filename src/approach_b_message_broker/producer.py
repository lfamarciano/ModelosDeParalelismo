import pandas as pd
import json
from celery import Celery, group
from celery.exceptions import TimeoutError
import time
import os
from pathlib import Path

# Funções para a corretude da solução
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
def validar_corretude(df_processado):
    gabarito_path = Path("data/anomalias_reais.csv")
    if not gabarito_path.exists(): return None
    gabarito_df = pd.read_csv(gabarito_path, parse_dates=["timestamp"])
    detectadas_df = df_processado.melt(id_vars=["timestamp", "id_estacao"], value_vars=["temperatura_anormal", "umidade_anormal", "pressao_anormal"], var_name="sensor_anomalo", value_name="is_anormal")
    detectadas_df = detectadas_df[detectadas_df["is_anormal"]]
    detectadas_df["sensor_anomalo"] = detectadas_df["sensor_anomalo"].str.replace("_anormal", "")
    merged_df = pd.merge(gabarito_df, detectadas_df, on=["timestamp", "id_estacao", "sensor_anomalo"], how='outer', indicator=True)
    return {
        "verdadeiros_positivos": len(merged_df[merged_df['_merge'] == 'both']),
    }

# Configuração do Celery
broker_url = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672//")
app = Celery('tasks', broker=broker_url, backend='rpc://')

output_path = "data/tempo_execucao.json"

from tasks import (
    calcular_percentual_anomalias,
    calcular_periodos_coocorrencia,
    calcular_media_movel,
)

df = pd.read_csv("data/dados_meteorologicos.csv", parse_dates=["timestamp"])

start_time = time.perf_counter()

# Lançar tarefas de percentual de anomalias e coocorrências
tasks_anomalias = []
tasks_cooc = []

for _, g in df.groupby("id_estacao"):
    # Converte o dataframe do grupo para JSON para ser serializável
    g_json = g.to_json(orient="records", date_format="iso")
    tasks_anomalias.append(calcular_percentual_anomalias.s(g_json))
    tasks_cooc.append(calcular_periodos_coocorrencia.s(g_json))

# Lançar tarefas de média móvel por região
tasks_moving_avg = []
for _, g in df.groupby("regiao"):
    g_json = g.to_json(orient="records", date_format="iso")
    tasks_moving_avg.append(calcular_media_movel.s(g_json))

# Agrupa todas as tarefas para execução e aguarda a conclusão
all_tasks = group(tasks_anomalias + tasks_cooc + tasks_moving_avg)
result = all_tasks.apply_async()

# Define o tempo de execução como -1 (erro) por padrão
duration_ms = -1.0

try:
    # Se as tarefas não terminarem nesse tempo, uma exceção TimeoutError será levantada,
    # impedindo que o programa congele indefinidamente.
    print("Aguardando a conclusão das tarefas do Celery (timeout em 600s)...")
    result.get(timeout=600)
    
    # Se get() retornar sem erro, o processamento foi bem-sucedido
    end_time = time.perf_counter()
    duration_ms = (end_time - start_time) * 1000
    print(f"Processamento com Celery + RabbitMQ finalizado com sucesso.")

except TimeoutError:
    print("\nERRO CRÍTICO: O processamento com Celery excedeu o tempo limite!")
    print("Isso pode indicar que os workers estão sobrecarregados ou travaram.")
    # Tenta cancelar as tarefas pendentes para limpar os recursos
    result.revoke(terminate=True)

except Exception as e:
    print(f"\nERRO CRÍTICO: Uma exceção ocorreu durante a espera dos resultados do Celery: {e}")
    result.revoke(terminate=True)
    
# Validação e salvamento do resultado combinado
df_validacao = detectar_anomalias(df)
corretude_results = validar_corretude(df_validacao)
final_results = {"tempo": duration_ms, "corretude": corretude_results}
with open("data/tempo_execucao.json", "w") as f:
    json.dump(final_results, f)
    
os.chmod(output_path, 0o666)

# Salva os resultados
# pd.DataFrame(sum(result_anomalias, [])).to_csv("data/percentuais_anomalias.csv", index=False)
# pd.DataFrame(result_cooc).to_csv("data/periodos_coocorrencia.csv", index=False)

# Média móvel pode gerar múltiplas tabelas que devem ser concatenadas
# media_df = pd.DataFrame(sum(result_moving_avg, []))
# media_df.to_csv("data/media_movel_regiao.csv", index=False)

print("Processamento com Celery + RabbitMQ finalizado.")
print(f"Tempo total: {duration_ms:.2f} ms")

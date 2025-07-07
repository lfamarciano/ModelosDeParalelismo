import pandas as pd
import json
from celery import Celery, group
import time
import os

broker_url = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672//")
app = Celery('tasks', broker=broker_url, backend='rpc://')

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
    print("Aguardando a conclusão das tarefas do Celery (timeout em 360s)...")
    result.get(timeout=360)
    
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
    
with open("data/tempo_execucao.json", "w") as f:
    json.dump({"tempo": duration_ms}, f)

# Salva os resultados
# pd.DataFrame(sum(result_anomalias, [])).to_csv("data/percentuais_anomalias.csv", index=False)
# pd.DataFrame(result_cooc).to_csv("data/periodos_coocorrencia.csv", index=False)

# Média móvel pode gerar múltiplas tabelas que devem ser concatenadas
# media_df = pd.DataFrame(sum(result_moving_avg, []))
# media_df.to_csv("data/media_movel_regiao.csv", index=False)

print("Processamento com Celery + RabbitMQ finalizado.")
print(f"Tempo total: {duration_ms:.2f} ms")

import pandas as pd
import json
from approach_b_message_broker.tasks import (
    calcular_percentual_anomalias,
    calcular_periodos_coocorrencia,
    calcular_media_movel,
)
from celery import group
import time

df = pd.read_csv("../data/dados_meteorologicos.csv", parse_dates=["timestamp"])

start_time = time.perf_counter()

# Lançar tarefas de percentual de anomalias e coocorrências
tasks_anomalias = []
tasks_cooc = []

for _, g in df.groupby("id_estacao"):
    g_json = g.to_json(orient="records", date_format="iso")
    tasks_anomalias.append(calcular_percentual_anomalias.s(g_json))
    tasks_cooc.append(calcular_periodos_coocorrencia.s(g_json))

# Lançar tarefas de média móvel por região
tasks_moving_avg = []
for _, g in df.groupby("regiao"):
    g_json = g.to_json(orient="records", date_format="iso")
    tasks_moving_avg.append(calcular_media_movel.s(g_json))

# Executa em paralelo com group
result_anomalias = group(tasks_anomalias)().get()
result_cooc = group(tasks_cooc)().get()
result_moving_avg = group(tasks_moving_avg)().get()

end_time = time.perf_counter()
duration_ms = (end_time - start_time) * 1000
with open("../data/tempo_execucao.json", "w") as f:
    json.dump({"tempo": duration_ms}, f)

# Salva os resultados
# pd.DataFrame(sum(result_anomalias, [])).to_csv("../data/percentuais_anomalias.csv", index=False)
# pd.DataFrame(result_cooc).to_csv("../data/periodos_coocorrencia.csv", index=False)

# Média móvel pode gerar múltiplas tabelas que devem ser concatenadas
# media_df = pd.DataFrame(sum(result_moving_avg, []))
# media_df.to_csv("../data/media_movel_regiao.csv", index=False)

print("Processamento com Celery + RabbitMQ finalizado.")
print(f"Tempo total: {duration_ms:.2f} ms")

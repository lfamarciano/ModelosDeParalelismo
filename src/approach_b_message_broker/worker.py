# antes de rodar garanta que o RabbitMQ e Redis estão rodando
# > docker-compose up rabbitmq redis
# > docker-compose up --scale worker=4 --no-recreate
import pika
import redis
import pandas as pd
import json
import time
from pathlib import Path

# Carrega todo o DataFrame na memória uma única vez quando o worker inicia
DATA_PATH = Path("data/dados_meteorologicos.csv")
ALL_DATA = pd.read_csv(DATA_PATH)
ALL_DATA['timestamp'] = pd.to_datetime(ALL_DATA['timestamp'])

def is_anomaly(series):
    mean = series.mean()
    std = series.std()
    if std == 0: return pd.Series([False] * len(series), index=series.index)
    lower_bound = mean - 3 * std
    upper_bound = mean + 3 * std
    return ~series.between(lower_bound, upper_bound)

def process_station(station_id: str):
    """
    Função que realiza o cálculo para uma única estação.
    """
    station_df = ALL_DATA[ALL_DATA['id_estacao'] == station_id].copy()
    if station_df.empty:
        return None

    # Métricas 1 e 3
    anomaly_mask = station_df[['temperatura', 'umidade', 'pressao']].apply(is_anomaly)
    percentual_anomalias = (anomaly_mask.mean() * 100).to_dict()

    periods = anomaly_mask.groupby(pd.Grouper(key='timestamp', freq='10min')).any()
    concurrent_anomalies = periods.sum(axis=1)
    num_concurrent_periods = (concurrent_anomalies > 1).sum()

    result = {
        "resultados_por_estacao": {
            station_id: {
                "percentual_anomalias": percentual_anomalias,
                "periodos_concorrentes": int(num_concurrent_periods)
            }
        }
    }
    return result

def main():
    # Conexão com RabbitMQ e Redis
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    redis_conn = redis.Redis(host='redis', port=6379, db=0)

    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Aguardando por tarefas. Para sair, pressione CTRL+C')

    def callback(ch, method, properties, body):
        station_id = body.decode()
        print(f" [x] Recebido '{station_id}'")
        
        start_time = time.time()
        result = process_station(station_id)
        end_time = time.time()
        
        if result:
            # Salva o resultado no Redis
            redis_conn.hset("approach_b_results", station_id, json.dumps(result))
        
        print(f" [x] Concluído '{station_id}' em {end_time - start_time:.4f} segundos.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrompido')
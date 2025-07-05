# antes de rodar garanta que o RabbitMQ e Redis estão rodando
# > docker-compose up rabbitmq redis
# > docker-compose run --rm python-app python src/approach_b_message_broker/producer.py
import pika
import pandas as pd
from pathlib import Path

def main():
    # Carrega os dados para pegar os IDs únicos das estações
    data_path = Path("data/dados_meteorologicos.csv")
    if not data_path.exists():
        print(f"Erro: Arquivo de dados não encontrado em {data_path}")
        return
        
    df = pd.read_csv(data_path)
    station_ids = df['id_estacao'].unique()

    # Conexão com o RabbitMQ (rodando em outro contêiner)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Declara uma fila durável chamada 'task_queue'
    channel.queue_declare(queue='task_queue', durable=True)
    
    print(f"Enviando {len(station_ids)} estações para a fila de tarefas...")

    for station_id in station_ids:
        # Publica cada ID de estação como uma mensagem na fila
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=station_id,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(f" [x] Enviado '{station_id}'")

    connection.close()
    print("Todas as tarefas foram enviadas.")

if __name__ == '__main__':
    main()
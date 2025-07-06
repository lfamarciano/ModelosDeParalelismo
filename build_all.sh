echo "Iniciando build de todas as imagens Docker..."

docker build -t data-generator ./src/data_generator
docker build -t local-processing ./src/approach_a_local_processing
docker build -t message-broker ./src/approach_b_message_broker
docker build -t spark-processing ./src/approach_c_spark_processing

echo "Build concluído!"
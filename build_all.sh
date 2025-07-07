# Este script constrói todas as imagens Docker necessárias para o projeto.

echo "Building data-generator image..."
docker build -t data-generator -f src/data_generator/Dockerfile .

echo "Building local-processing image..."
docker build -t local-processing -f src/approach_a_local_processing/Dockerfile .

echo "Building message-broker image..."
docker build -t message-broker -f src/approach_b_message_broker/Dockerfile .

echo "Building spark-processing image..."
docker build -t spark-processing -f src/approach_c_spark_processing/Dockerfile .

echo "All images built successfully."
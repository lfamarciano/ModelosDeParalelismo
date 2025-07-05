# Build
FROM gcc:12.3.0 as builder

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia apenas o código-fonte e as dependências necessárias para compilar
COPY main.cpp .
COPY include/ ./include/

# Compila o programa com as mesmas flags de otimização
# O executável será gerado dentro da imagem, em /app/processador_cpp
RUN g++ -std=c++17 -O3 -pthread main.cpp -o processador_cpp


# Usamos uma imagem base do Ubuntu, muito menor, para o contêiner final
FROM ubuntu:22.04

WORKDIR /app

# Copia APENAS o executável compilado do estágio 'builder'
COPY --from=builder /app/processador_cpp .

# Define o comando padrão que será executado se o contêiner rodar
ENTRYPOINT ["./processador_cpp"]
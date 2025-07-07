import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import subprocess
import time
import os
import json

# Caminho onde os tempos serão armazenados
TEMPOS_PATH = "experimentos_tempos.csv"
OUTPUT_PATH = "data/tempo_execucao.json"

# Abordagens disponíveis
ABORDAGENS = [
    "local-processing",
    "message-broker",
    "spark-processing"
]

def inicializar_csv():
    """Garante que o arquivo de resultados exista."""
    if not os.path.exists(TEMPOS_PATH):
        colunas = [
            "abordagem", "paralelismo", "tempo_seg", "n_estacoes", "n_eventos", "detectadas_ok"
        ]
        df = pd.DataFrame(columns=colunas)
        df.to_csv(TEMPOS_PATH, index=False)

def limpar_resultados():
    """Apaga os resultados anteriores."""
    if os.path.exists(TEMPOS_PATH):
        os.remove(TEMPOS_PATH)
    inicializar_csv()

def carregar_resultados():
    """Carrega os resultados do arquivo CSV."""
    if not os.path.exists(TEMPOS_PATH):
        return pd.DataFrame(columns=["abordagem", "paralelismo", "tempo_seg", "n_estacoes", "n_eventos" ,"detectadas_ok"])
    return pd.read_csv(TEMPOS_PATH)

def ler_resultado_experimento():
    """Lê o tempo e os dados de corretude do arquivo JSON."""
    try:
        with open(OUTPUT_PATH, "r") as f:
            data = json.load(f)
            tempo_seg = data.get("tempo", -1.0) / 1000 if data.get("tempo", -1.0) > 0 else -1.0
            return {
                "tempo": tempo_seg,
                "corretude": data.get("corretude", None)
            }
    except (FileNotFoundError, json.JSONDecodeError):
        return {"tempo": -1.0, "corretude": None}

def rodar_experimento(abordagem, paralelismo, n_estacoes, n_eventos, status_placeholder):
    """Executa um único teste para uma abordagem com um nível de paralelismo."""
    status_placeholder.info(f"Executando: {abordagem} com paralelismo {paralelismo}...")

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    shared_volume = f"{os.getcwd()}/data:/app/data"
    # O nome da rede geralmente é <pasta_do_projeto>_default. Verifique com 'docker network ls'.
    network_name = "modelosdeparalelismo_default"

    # Esconde a saída dos comandos docker para não poluir o console
    capture_output_args = {'capture_output': True, 'text': True}

    if abordagem == "local-processing":
        cmd = ["docker", "run", "--rm", "-v", shared_volume,
               "local-processing", "python", "process_local.py", str(paralelismo)]
        subprocess.run(cmd, **capture_output_args)

    elif abordagem == "message-broker":
        subprocess.run(["docker", "compose", "up", "-d", "rabbitmq"], **capture_output_args)
        time.sleep(10) # Espera o RabbitMQ iniciar

        worker_name = f"celery-worker-{paralelismo}"
        worker_cmd = [
            "docker", "run", "-d", "--rm", "--name", worker_name,
            "-v", shared_volume, "--network", network_name,
            "-e", "CELERY_BROKER_URL=amqp://guest:guest@rabbitmq:5672//",
            "message-broker", "celery", "-A", "tasks", "worker",
            f"--concurrency={paralelismo}", "--loglevel=info"
        ]
        subprocess.run(worker_cmd, **capture_output_args)
        time.sleep(5) # Espera os workers se registrarem

        producer_cmd = [
            "docker", "run", "--rm",
            "-v", shared_volume, "--network", network_name,
            "-e", "CELERY_BROKER_URL=amqp://guest:guest@rabbitmq:5672//",
            "message-broker", "python", "producer.py"
        ]
        subprocess.run(producer_cmd, **capture_output_args)

        subprocess.run(["docker", "stop", worker_name], **capture_output_args)

    elif abordagem == "spark-processing":
        cmd = ["docker", "run", "--rm",
               "-v", shared_volume,
               "-e", f"SPARK_PARALLELISM={paralelismo}",
               "spark-processing", "python3", "process_spark.py"]
        subprocess.run(cmd, **capture_output_args)

    resultado = ler_resultado_experimento()
    tempo_execucao = resultado["tempo"]
    corretude = resultado["corretude"]
    
    # Prepara a nova linha com todos os dados
    new_row_data = {
        "abordagem": abordagem,
        "paralelismo": paralelismo,
        "tempo_seg": tempo_execucao,
        "n_estacoes": n_estacoes,
        "n_eventos": n_eventos,
        "detectadas_ok": corretude.get("verdadeiros_positivos") if corretude else "N/A",
    }

    df = carregar_resultados()
    # Concatena a nova linha ao DataFrame de resultados
    df = pd.concat([df, pd.DataFrame([new_row_data])], ignore_index=True)
    df.to_csv(TEMPOS_PATH, index=False)
    
    status_placeholder.success(f"Finalizado: {abordagem} (paralelismo {paralelismo}) em {tempo_execucao:.2f}s")
    return df

def iniciar_experimentos(paralelismos, n_eventos, n_estacoes):
    """Orquestra a execução de todos os experimentos."""
    st.info("🔧 Gerando dados simulados...")
    subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}/data:/app/data", "data-generator",
                    "python", "data_generator.py", str(n_estacoes), str(n_eventos), "0.02"],
                   capture_output=True, text=True)
    st.success("Dados gerados com sucesso!")

    # Placeholders para os elementos que serão atualizados em tempo real
    status_placeholder = st.empty()
    results_placeholder = st.empty()
    graph_placeholder = st.empty()
    
    df_resultados = carregar_resultados()

    for p in paralelismos:
        for abordagem in ABORDAGENS:
            df_resultados = rodar_experimento(abordagem, p, n_estacoes, n_eventos, status_placeholder)
            
            # Atualiza a tabela de resultados na tela
            results_placeholder.dataframe(df_resultados.sort_values(by=["abordagem", "paralelismo"]))
            
            # Atualiza o gráfico na tela
            with graph_placeholder.container():
                st.subheader("Gráfico: Tempo vs. Paralelismo")
                fig, ax = plt.subplots()
                for ab in df_resultados["abordagem"].unique():
                    dados = df_resultados[df_resultados["abordagem"] == ab].sort_values("paralelismo")
                    dados = dados[dados["tempo_seg"] > 0] # Filtra execuções com erro
                    ax.plot(dados["paralelismo"], dados["tempo_seg"], marker='o', linestyle='-', label=ab)
                ax.set_xlabel("Grau de Paralelismo (Escala Log)")
                ax.set_ylabel("Tempo de Execução (segundos)")
                ax.set_title("Desempenho das Abordagens de Paralelismo")
                ax.set_xscale('log', base=2)
                ax.set_xticks(paralelismos)
                ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
                ax.legend()
                ax.grid(True, which="both", ls="--")
                st.pyplot(fig)

    status_placeholder.success("Todos os experimentos foram finalizados!")

# Interface do Streamlit
st.set_page_config(layout="wide")
st.title("Painel de Experimentos de Processamento Paralelo")
inicializar_csv()

# Parâmetros do experimento
st.sidebar.header("Parâmetros do Experimento")
max_paralelismo_exp = st.sidebar.number_input("Expoente máximo de paralelismo (2^n)", min_value=0, value=3, step=1, key="max_paralelismo")
paralelismos = [2**i for i in range(max_paralelismo_exp + 1)]
st.sidebar.write(f"Níveis de paralelismo a testar: {paralelismos}")
n_eventos = st.sidebar.number_input("Nº eventos por estação", min_value=1000, value=10000, step=1000, key="n_eventos")
n_estacoes = st.sidebar.number_input("Nº de estações", min_value=1, value=12, step=1, key="n_estacoes")

# Botões de controle
st.sidebar.header("Controles")
if st.sidebar.button("🚀 Iniciar Experimento"):
    limpar_resultados()
    iniciar_experimentos(paralelismos, n_eventos, n_estacoes)

if st.sidebar.button("🧹 Limpar Resultados"):
    limpar_resultados()
    st.success("Resultados apagados com sucesso.")
    time.sleep(1) # Pequena pausa para o usuário ver a mensagem
    st.rerun()

# Exibição inicial dos resultados
st.header("Resultados")
df_inicial = carregar_resultados()
if not df_inicial.empty:
    st.dataframe(df_inicial.sort_values(by=["abordagem", "paralelismo"]))
else:
    st.info("Nenhum resultado para exibir. Inicie um experimento na barra lateral.")
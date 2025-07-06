import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import subprocess
import time
import os
import json

# Caminho onde os tempos serão armazenados (simples CSV)
TEMPOS_PATH = "./experimentos_tempos.csv"
OUTPUT_PATH = "./data/tempo_execucao.json"

# Abordagens disponíveis
ABORDAGENS = [
    "local-processing",
    "message-broker",
    "spark-processing"
]

def inicializar_csv():
    if not os.path.exists(TEMPOS_PATH):
        df = pd.DataFrame(columns=["abordagem", "paralelismo", "tempo_seg", "n_estacoes", "n_eventos"])
        df.to_csv(TEMPOS_PATH, index=False)

def limpar_resultados():
    if os.path.exists(TEMPOS_PATH):
        os.remove(TEMPOS_PATH)
    inicializar_csv()

def carregar_resultados():
    if os.path.isdir(TEMPOS_PATH):
        st.error(f"O caminho '{TEMPOS_PATH}' é um diretório, mas deveria ser um arquivo CSV. Remova ou renomeie a pasta.")
        return pd.DataFrame(columns=["abordagem", "paralelismo", "tempo_seg", "n_estacoes", "n_eventos"])
    if os.path.exists(TEMPOS_PATH):
        return pd.read_csv(TEMPOS_PATH)
    else:
        return pd.DataFrame(columns=["abordagem", "paralelismo", "tempo_seg", "n_estacoes", "n_eventos"])

def ler_tempo_execucao():
    try:
        with open(OUTPUT_PATH, "r") as f:
            data = json.load(f)
            return data.get("tempo", None)
    except:
        return None

def rodar_experimento(abordagem, paralelismo, n_estacoes, n_eventos):
    st.session_state[f"{abordagem}-{paralelismo}"] = "Executando..."

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    if abordagem == "local-processing":
        subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}/data:/app/data",
                        "local-processing", "python", "process_local.py", str(paralelismo)])

    elif abordagem == "message-broker":
        subprocess.run(["docker", "compose", "up", "-d", "rabbitmq"])
        subprocess.run([
            "docker", "compose", "run", "--rm",
            "-e", f"CELERY_CONCURRENCY={paralelismo}",
            "message-broker"
        ])
        subprocess.run(["docker", "compose", "stop", "message-broker"])

    elif abordagem == "spark-processing":
        subprocess.run(["docker", "run", "--rm",
                        "-v", f"{os.getcwd()}/data:/app/data",
                        "-e", f"SPARK_PARALLELISM={paralelismo}",
                        "spark-processing"])

    tempo_execucao = ler_tempo_execucao()
    if tempo_execucao is None:
        tempo_execucao = -1

    df = carregar_resultados()
    df = pd.concat([
        df,
        pd.DataFrame([{ "abordagem": abordagem, "paralelismo": paralelismo, "tempo_seg": tempo_execucao,
                         "n_estacoes": n_estacoes, "n_eventos": n_eventos }])
    ])
    df.to_csv(TEMPOS_PATH, index=False)
    st.session_state[f"{abordagem}-{paralelismo}"] = f"✅ {tempo_execucao:.2f} s"

def iniciar_experimentos(paralelismos, n_eventos, n_estacoes):
    # Gerar os dados primeiro
    st.info("🔧 Gerando dados simulados...")
    subprocess.run(["docker", "compose", "run", "--rm", "data-generator",
                    "python", "data_generator.py", str(n_estacoes), str(n_eventos), "0.02"])

    # Rodar abordagens sequencialmente
    for p in paralelismos:
        for abordagem in ABORDAGENS:
            rodar_experimento(abordagem, p, n_estacoes, n_eventos)

# --- STREAMLIT APP ---
st.title("Painel de Experimentos de Processamento Paralelo")
inicializar_csv()

col1, col2, col3 = st.columns(3)
with col1:
    max_paralelismo = st.number_input("Grau máximo de paralelismo", min_value=1, value=4, step=1)
with col2:
    n_eventos = st.number_input("Nº eventos por estação", min_value=1000, value=10000, step=1000)
with col3:
    n_estacoes = st.number_input("Nº de estações", min_value=1, value=20, step=1)

paralelismos = [2 ** i for i in range(int(max_paralelismo).bit_length()) if 2 ** i <= max_paralelismo]

col_a, col_b = st.columns(2)
with col_a:
    if st.button("Iniciar Experimento"):
        iniciar_experimentos(paralelismos, n_eventos, n_estacoes)
with col_b:
    if st.button("Limpar Resultados"):
        limpar_resultados()
        st.success("Resultados apagados com sucesso.")

# Resultados em tempo real
st.subheader("Resultados")
df = carregar_resultados()
if not df.empty:
    st.dataframe(df.sort_values(by=["abordagem", "paralelismo"]))

    st.subheader("Gráfico: Tempo vs. Paralelismo")
    fig, ax = plt.subplots()
    for abordagem in ABORDAGENS:
        dados = df[df["abordagem"] == abordagem]
        ax.plot(dados["paralelismo"], dados["tempo_seg"], label=abordagem)
    ax.set_xlabel("Paralelismo")
    ax.set_ylabel("Tempo (ms)")
    ax.legend()
    st.pyplot(fig)

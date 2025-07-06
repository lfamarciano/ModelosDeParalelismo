import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import subprocess
import threading
import time
import os

# Caminho para o csv onde os tempos serão armazenados
TEMPOS_PATH = "./experimentos_tempos.csv"

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

@st.cache_data(show_spinner=False)
def carregar_resultados():
    if os.path.exists(TEMPOS_PATH):
        return pd.read_csv(TEMPOS_PATH)
    else:
        return pd.DataFrame(columns=["abordagem", "paralelismo", "tempo_seg", "n_estacoes", "n_eventos"])

def rodar_experimento(abordagem, paralelismo, n_estacoes, n_eventos):
    st.session_state[f"{abordagem}-{paralelismo}"] = "Executando..."
    start = time.perf_counter()

    if abordagem == "local-processing":
        subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}/data:/app/data",
                        "local-processing", "python", "process_local.py", str(paralelismo)])

    elif abordagem == "message-broker":
        subprocess.run(["docker", "compose", "up", "--abort-on-container-exit", "message-broker"], stdout=subprocess.DEVNULL)

    elif abordagem == "spark-processing":
        subprocess.run(["docker", "run", "--rm",
                        "-v", f"{os.getcwd()}/data:/app/data",
                        "-e", f"SPARK_PARALLELISM={paralelismo}",
                        "spark-processing"])

    fim = time.perf_counter()
    duracao = fim - start

    # Atualiza CSV
    df = carregar_resultados()
    df = pd.concat([
        df,
        pd.DataFrame([{ "abordagem": abordagem, "paralelismo": paralelismo, "tempo_seg": duracao,
                         "n_estacoes": n_estacoes, "n_eventos": n_eventos }])
    ])
    df.to_csv(TEMPOS_PATH, index=False)
    st.session_state[f"{abordagem}-{paralelismo}"] = f"✅ {duracao:.2f} s"

def iniciar_experimentos(paralelismos, n_eventos, n_estacoes):
    subprocess.run(["docker", "compose", "run", "--rm", "data-generator",
                    "python", "data_generator.py", str(n_estacoes), str(n_eventos), "0.02"])

    threads = []
    for p in paralelismos:
        for abordagem in ABORDAGENS:
            t = threading.Thread(target=rodar_experimento, args=(abordagem, p, n_estacoes, n_eventos))
            t.start()
            threads.append(t)

    for t in threads:
        t.join()

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
        threading.Thread(target=iniciar_experimentos, args=(paralelismos, n_eventos, n_estacoes)).start()
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
    ax.set_ylabel("Tempo (s)")
    ax.legend()
    st.pyplot(fig)

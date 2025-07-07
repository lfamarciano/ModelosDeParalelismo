import pandas as pd
import numpy as np
from multiprocessing import Process, Manager
import sys
import time
import json
from pathlib import Path

THRESHOLDS = {
    "temperatura": (-10, 45),
    "umidade": (0, 100),
    "pressao": (940, 1060)
}

def detectar_anomalias(df):
    for sensor in ["temperatura", "umidade", "pressao"]:
        min_val, max_val = THRESHOLDS[sensor]
        df[f"{sensor}_anormal"] = ~df[sensor].between(min_val, max_val)
    return df

def validar_corretude(df_processado):
    gabarito_path = Path("data/anomalias_reais.csv")
    if not gabarito_path.exists(): return None
    gabarito_df = pd.read_csv(gabarito_path, parse_dates=["timestamp"])
    detectadas_df = df_processado.melt(id_vars=["timestamp", "id_estacao"], value_vars=["temperatura_anormal", "umidade_anormal", "pressao_anormal"], var_name="sensor_anomalo", value_name="is_anormal")
    detectadas_df = detectadas_df[detectadas_df["is_anormal"]]
    detectadas_df["sensor_anomalo"] = detectadas_df["sensor_anomalo"].str.replace("_anormal", "")
    merged_df = pd.merge(gabarito_df, detectadas_df, on=["timestamp", "id_estacao", "sensor_anomalo"], how='outer', indicator=True)
    return {
        "verdadeiros_positivos": len(merged_df[merged_df['_merge'] == 'both']),
    }

def worker_anomalias(estacao_df, output_list):
    id_est = estacao_df["id_estacao"].iloc[0]
    resultados = []
    for sensor in ["temperatura", "umidade", "pressao"]:
        total = len(estacao_df)
        anomalias = estacao_df[f"{sensor}_anormal"].sum()
        resultados.append({
            "id_estacao": id_est,
            "sensor": sensor,
            "percentual_anomalias": 100 * anomalias / total
        })
    output_list.extend(resultados)

def worker_coocorrencia(estacao_df, output_list):
    estacao_df = estacao_df.sort_values("timestamp")
    estacao_df.set_index("timestamp", inplace=True)
    grupo = estacao_df[["temperatura_anormal", "umidade_anormal", "pressao_anormal"]]
    rolling = grupo.rolling("10min").sum()
    mask = (rolling > 0).sum(axis=1) > 1
    num_periodos = mask.sum()
    output_list.append({
        "id_estacao": estacao_df["id_estacao"].iloc[0],
        "periodos_multianomalias_10min": num_periodos
    })

def worker_media_movel(regiao_df, output_list):
    regiao_df = regiao_df.sort_values("timestamp").set_index("timestamp")
    regiao = regiao_df["regiao"].iloc[0]
    regiao_df = regiao_df[~(regiao_df["temperatura_anormal"] |
                            regiao_df["umidade_anormal"] |
                            regiao_df["pressao_anormal"])]
    rolling = regiao_df[["temperatura", "umidade", "pressao"]].rolling("10min").mean()
    rolling["regiao"] = regiao
    rolling["timestamp"] = rolling.index
    output_list.append(rolling.reset_index(drop=True))

def run_with_limited_processes(func, grupos, output_list, max_proc):
    processos = []
    for grupo in grupos:
        p = Process(target=func, args=(grupo, output_list))
        p.start()
        processos.append(p)
        while len(processos) >= max_proc:
            for p in processos:
                if not p.is_alive():
                    processos.remove(p)
            time.sleep(0.1)
    # Espera os últimos
    for p in processos:
        p.join()

def processa_anomalias(df, manager, max_proc):
    resultados = manager.list()
    grupos = [g for _, g in df.groupby("id_estacao")]
    run_with_limited_processes(worker_anomalias, grupos, resultados, max_proc)
    return list(resultados)

def processa_coocorrencias(df, manager, max_proc):
    resultados = manager.list()
    grupos = [g for _, g in df.groupby("id_estacao")]
    run_with_limited_processes(worker_coocorrencia, grupos, resultados, max_proc)
    return list(resultados)

def processa_medias_moveis(df, manager, max_proc):
    resultados = manager.list()
    grupos = [g for _, g in df.groupby("regiao")]
    run_with_limited_processes(worker_media_movel, grupos, resultados, max_proc)
    return pd.concat(list(resultados))

def main():
    if len(sys.argv) < 2:
        print("Uso: python process_local.py <num_processos>")
        sys.exit(1)

    max_processes = int(sys.argv[1])
    df = pd.read_csv("data/dados_meteorologicos.csv", parse_dates=["timestamp"])
    df = detectar_anomalias(df)

    with Manager() as manager:
        start_time = time.perf_counter()
        print(f"Usando até {max_processes} processos paralelos...\n")

        print("Processando percentuais de anomalias...")
        processa_anomalias(df, manager, max_processes)

        print("Processando períodos de coocorrência...")
        processa_coocorrencias(df, manager, max_processes)

        print("Processando médias móveis por região...")
        processa_medias_moveis(df, manager, max_processes)
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        
        # pd.DataFrame(anomalias).to_csv("data/percentuais_anomalias.csv", index=False)
        # pd.DataFrame(coocorrencias).to_csv("data/periodos_coocorrencia.csv", index=False)
        # medias_moveis.to_csv("data/media_movel_regiao.csv", index=False)
        # Validação e salvamento do resultado combinado
        corretude_results = validar_corretude(df)
        final_results = {"tempo": duration_ms, "corretude": corretude_results}
        with open("data/tempo_execucao.json", "w") as f:
            json.dump(final_results, f)

    print("\nProcessamento finalizado com multiprocessamento local.")
    print(f"Tempo total: {duration_ms:.2f} ms")

if __name__ == "__main__":
    main()
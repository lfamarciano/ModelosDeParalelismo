import pandas as pd
from pathlib import Path
import json
import time

def is_anomaly(series):
    """Identifica anomalias em uma série de dados (valores fora de média ± 3*std)."""
    mean = series.mean()
    std = series.std()
    # Evita divisão por zero se todos os valores forem iguais
    if std == 0:
        return pd.Series([False] * len(series), index=series.index)
    lower_bound = mean - 3 * std
    upper_bound = mean + 3 * std
    return ~series.between(lower_bound, upper_bound)

def calculate_metrics_sequentially(df: pd.DataFrame):
    """
    Calcula as três métricas necessárias de forma sequencial usando Pandas.
    Esta é a implementação de referência.
    """
    metrics = {}
    
    #  Métrica 1: Percentual de anomalias por sensor por estação 
    anomaly_mask_df = df.groupby('id_estacao')[['temperatura', 'umidade', 'pressao']].transform(is_anomaly)
    percentual_anomalias = anomaly_mask_df.groupby(df['id_estacao']).mean() * 100
    metrics['percentual_anomalias'] = percentual_anomalias


    #  Métrica 2: Média móvel por região (excluindo anomalias) 
    is_any_anomaly = anomaly_mask_df.any(axis=1)
    clean_df = df[~is_any_anomaly].copy()
    
    # Garante que os dados estão ordenados por tempo dentro de cada região antes de calcular a média móvel
    clean_df = clean_df.sort_values(by=['regiao', 'timestamp'])
    
    rolling_cols = ['temperatura', 'umidade', 'pressao']
    # Calcula a média móvel
    moving_avg_values = clean_df.groupby('regiao')[rolling_cols].rolling(window=10, min_periods=1).mean()
    
    # Remove o MultiIndex para poder juntar os dados de volta
    moving_avg_values = moving_avg_values.reset_index(level=0, drop=True)

    # Junta os resultados da média móvel ao DataFrame limpo
    media_movel_df = clean_df.join(moving_avg_values.rename(columns=lambda c: f"{c}_media_movel"))
    metrics['media_movel_regiao'] = media_movel_df


    #  Métrica 3: Períodos com anomalias concorrentes por estação 
    df_anomalies_only = df.join(anomaly_mask_df.rename(
        columns=lambda c: f"{c}_anomalo"
    ))
    
    periods = df_anomalies_only.groupby([
        'id_estacao',
        pd.Grouper(key='timestamp', freq='10min')
    ])[['temperatura_anomalo', 'umidade_anomalo', 'pressao_anomalo']].any()

    concurrent_anomalies = periods.sum(axis=1)
    concurrent_periods = concurrent_anomalies[concurrent_anomalies > 1]
    
    num_concurrent_periods = concurrent_periods.groupby('id_estacao').count()
    metrics['periodos_concorrentes'] = num_concurrent_periods.to_frame(name='num_periodos_concorrentes')

    return metrics

if __name__ == '__main__':
    # Carrega os dados gerados
    data_path = Path("data/dados_meteorologicos.csv")
    if not data_path.exists():
        print(f"Arquivo de dados não encontrado em {data_path}.")
        print("Por favor, execute 'python src/data_generator.py' primeiro.")
    else:
        print(f"Carregando dados de {data_path}...")
        
        df_principal = pd.read_csv(data_path, parse_dates=['timestamp'])
        
        print("Calculando métricas (implementação de referência)...")
        start_time = time.perf_counter()
        resultados_df = calculate_metrics_sequentially(df_principal)
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        
        # --- Estruturação do JSON de Saída ---
        final_json_output = {
            "tempo_execucao_ms": duration_ms,
            "resultados_por_estacao": {},
            "media_movel_por_regiao": {}
        }
        
        # Estrutura os resultados das métricas 1 e 3
        all_stations = df_principal['id_estacao'].unique()
        metric_1 = resultados_df['percentual_anomalias'].to_dict('index')
        metric_3 = resultados_df['periodos_concorrentes']['num_periodos_concorrentes'].to_dict()

        for station in sorted(all_stations):
            final_json_output["resultados_por_estacao"][station] = {
                "percentual_anomalias": metric_1.get(station, {}),
                "periodos_concorrentes": metric_3.get(station, 0)
            }
            
        metric_2_df = resultados_df['media_movel_regiao']
        metric_2_df['timestamp'] = metric_2_df['timestamp'].astype(str)
        
        # Agrupa os resultados da média móvel por região
        for region, group_df in metric_2_df.groupby('regiao'):
            final_json_output["media_movel_por_regiao"][region] = group_df.to_dict('records')
        
        output_path = Path("data/resultado_referencia.json")
        with open(output_path, 'w') as f:
            json.dump(final_json_output, f, indent=4)
        
        print(f"\nResultados de referência salvos em: {output_path}")
        print(f"Tempo de execução: {duration_ms:.2f} ms")
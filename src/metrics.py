import pandas as pd
from pathlib import Path

def is_anomaly(series):
    """Identifica anomalias em uma série de dados (valores fora de média ± 3*std)."""
    mean = series.mean()
    std = series.std()
    lower_bound = mean - 3 * std
    upper_bound = mean + 3 * std
    return ~series.between(lower_bound, upper_bound)

def calculate_metrics_sequentially(df: pd.DataFrame):
    """
    Calcula as três métricas necessárias de forma sequencial usando pandas.
    Esta é a implementação de referência.
    """
    metrics = {}
    
    #  Métrica 1: Percentual de anomalias por sensor por estação 
    anomaly_cols = {}
    grouped_by_station = df.groupby('id_estacao')
    
    for station, station_df in grouped_by_station:
        anomaly_cols[('temperatura', station)] = is_anomaly(station_df['temperatura'])
        anomaly_cols[('umidade', station)] = is_anomaly(station_df['umidade'])
        anomaly_cols[('pressao', station)] = is_anomaly(station_df['pressao'])
    
    # Unifica os resultados de anomalias em um unico df
    anomaly_df = pd.concat(anomaly_cols).unstack(level=1)
    
    # Calcula o percentual
    percent_anomalies = anomaly_df.mean().groupby(level=1).apply(lambda x: x * 100)
    metrics['percentual_anomalias'] = percent_anomalies.unstack(level=0)


    #  Métrica 2: Média móvel por região (excluindo anomalias) 
    # Cria uma máscara para todas as anomalias
    df_with_anomalies = df.join(anomaly_df.droplevel(0, axis=1).any(axis=1).rename('is_any_anomaly'))
    
    # Remove as linhas com qualquer anomalia
    clean_df = df_with_anomalies[~df_with_anomalies['is_any_anomaly']].copy()

    # Calcula a média móvel de 10 minutos por região
    # O DataFrame já vem ordenado por timestamp do gerador
    moving_avg = clean_df.groupby('regiao')[['temperatura', 'umidade', 'pressao']].rolling(window=10, min_periods=1).mean()
    metrics['media_movel_regiao'] = moving_avg.reset_index()


    #  Métrica 3: Períodos com anomalias concorrentes por estação 
    # Usa o DataFrame que já tem a identificação de anomalias por sensor
    df_anomalies_only = df.join(anomaly_df.droplevel(0, axis=1))
    
    # Agrupa por estação e janelas de 10 minutos
    periods = df_anomalies_only.groupby([
        'id_estacao',
        pd.Grouper(key='timestamp', freq='10min')
    ])[['temperatura', 'umidade', 'pressao']].any() # Verifica se há alguma anomalia na janela

    # Conta quantos sensores tiveram anomalia na janela e filtra por > 1
    concurrent_anomalies = periods.sum(axis=1)
    concurrent_periods = concurrent_anomalies[concurrent_anomalies > 1]
    
    # Conta o número de períodos concorrentes por estação
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
        # Lendo o timestamp como data para o Grouper funcionar
        df_principal = pd.read_csv(data_path, parse_dates=['timestamp'])
        
        print("Calculando métricas (implementação de referência)...")
        resultados = calculate_metrics_sequentially(df_principal)

        print("\n--- RESULTADOS DE REFERÊNCIA ---")
        
        print("\n[Métrica 1: Percentual de Anomalias por Estação (%)]")
        print(resultados['percentual_anomalias'])

        print("\n[Métrica 3: Contagem de Períodos com Anomalias Concorrentes por Estação]")
        print(resultados['periodos_concorrentes'])
        
        print("\n[Métrica 2: Média Móvel por Região (últimos 5 registros)]")
        print(resultados['media_movel_regiao'].tail())
        
        print("\n--- FIM DOS RESULTADOS ---")
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
    Calcula as três métricas necessárias de forma sequencial usando Pandas.
    Esta é a implementação de referência.
    """
    metrics = {}
    
    # --- Métrica 1: Percentual de anomalias por sensor por estação ---
    # Primeiro, criamos um DataFrame que nos diz, para cada ponto, se ele é uma anomalia
    anomaly_mask_df = df.groupby('id_estacao')[['temperatura', 'umidade', 'pressao']].transform(is_anomaly)
    
    # Agora, calculamos a média (que para booleanos, é o percentual) e multiplicamos por 100
    # Agrupamos pelo id_estacao do DataFrame original para obter os resultados por estação
    percentual_anomalias = anomaly_mask_df.groupby(df['id_estacao']).mean() * 100
    metrics['percentual_anomalias'] = percentual_anomalias


    # --- Métrica 2: Média móvel por região (excluindo anomalias) ---
    # Cria uma máscara para identificar linhas que contêm QUALQUER anomalia
    is_any_anomaly = anomaly_mask_df.any(axis=1)
    
    # Remove as linhas com qualquer anomalia
    clean_df = df[~is_any_anomaly].copy()

    # Calcula a média móvel de 10 minutos por região
    # O DataFrame já vem ordenado por timestamp do gerador
    # Usamos include_groups=False para evitar um FutureWarning no Pandas 2.x
    moving_avg = (
        clean_df.groupby('regiao', group_keys=False, as_index=False)
        [['temperatura', 'umidade', 'pressao']]
        .rolling(window=10, min_periods=1)
        .mean()
    )
    # Adicionamos as colunas de identificação de volta para clareza
    metrics['media_movel_regiao'] = pd.concat([
        clean_df[['timestamp', 'id_estacao', 'regiao']].reset_index(drop=True),
        moving_avg.reset_index(drop=True)
    ], axis=1)


    # --- Métrica 3: Períodos com anomalias concorrentes por estação ---
    # Adicionamos a máscara de anomalias ao DataFrame original para facilitar o agrupamento
    df_anomalies_only = df.join(anomaly_mask_df.rename(
        columns=lambda c: f"{c}_anomalo"
    ))
    
    # Agrupa por estação e janelas de 10 minutos
    periods = df_anomalies_only.groupby([
        'id_estacao',
        pd.Grouper(key='timestamp', freq='10min')
    ])[['temperatura_anomalo', 'umidade_anomalo', 'pressao_anomalo']].any()

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
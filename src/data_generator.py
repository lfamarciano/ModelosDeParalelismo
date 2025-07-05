# > python src/data_generator.py 12 60 0.02
import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import time

def generate_data(num_stations: int, num_events_per_station: int, anomaly_percentage: float, start_date_str: str = "2025-07-01"):
    """
    Gera dados meteorológicos sintéticos para um número dinâmico de estações.

    Args:
        num_stations (int): O número de estações a serem simuladas.
        num_events_per_station (int): O número de amostras (eventos) a serem geradas por estação.
        anomaly_percentage (float): O percentual de amostras que devem ser anômalas (valor entre 0 e 1).
        start_date_str (str): A data de início para a geração dos dados no formato 'YYYY-MM-DD'.
    
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Um tuple contendo o DataFrame de dados meteorológicos
                                            e o DataFrame com o registro das anomalias geradas.
    """
    print(f"Iniciando geração de dados para {num_stations} estações com {num_events_per_station} eventos cada...")
    total_events = num_stations * num_events_per_station
    
    #  Pré-alocação de Arrays com NumPy 
    regions_list = ["Sudeste", "Nordeste", "Sul", "Norte", "Centro-Oeste"]
    all_timestamps = np.empty(total_events, dtype='datetime64[s]')
    all_station_ids = np.empty(total_events, dtype=object)
    all_regions = np.empty(total_events, dtype=object)
    all_temperatures = np.empty(total_events, dtype=float)
    all_humidities = np.empty(total_events, dtype=float)
    all_pressures = np.empty(total_events, dtype=float)

    # Preenchimento em Lotes
    timestamps_base = pd.to_datetime(np.arange(num_events_per_station), unit='m', origin=pd.Timestamp(start_date_str)).to_numpy()
    
    for i in range(num_stations):
        start_idx = i * num_events_per_station
        end_idx = (i + 1) * num_events_per_station
        
        station_id = f"STA-{i+1:03d}"
        region = regions_list[i % len(regions_list)]
        
        # Preenche as colunas de identificação
        all_timestamps[start_idx:end_idx] = timestamps_base
        all_station_ids[start_idx:end_idx] = station_id
        all_regions[start_idx:end_idx] = region

        # Gera dados numéricos de forma vetorizada
        time_component = np.linspace(0, 2 * np.pi, num_events_per_station)
        region_offset = i * 0.5
        
        temp_base = 25 + 8 * np.sin(time_component) + region_offset
        all_temperatures[start_idx:end_idx] = temp_base + np.random.normal(0, 0.5, num_events_per_station)

        humidity_base = 60 - 20 * np.sin(time_component)
        all_humidities[start_idx:end_idx] = np.clip(humidity_base + np.random.normal(0, 2, num_events_per_station), 0, 100)

        pressure_base = 1012 + 5 * np.sin(time_component / 2)
        all_pressures[start_idx:end_idx] = pressure_base + np.random.normal(0, 1, num_events_per_station)

    # Criação de um único DataFrame
    print("Construindo DataFrame final...")
    final_df = pd.DataFrame({
        "timestamp": all_timestamps,
        "id_estacao": all_station_ids,
        "regiao": all_regions,
        "temperatura": all_temperatures,
        "umidade": all_humidities,
        "pressao": all_pressures
    })

    # Injeção de anomalias de forma vetorizada
    print("Introduzindo anomalias...")
    num_anomalies = int(total_events * anomaly_percentage)
    if num_anomalies > 0:
        anomaly_indices = np.random.choice(final_df.index, size=num_anomalies, replace=False)
        sensors = ["temperatura", "umidade", "pressao"]
        sensors_to_alter = np.random.choice(sensors, size=num_anomalies)
        
        ground_truth_anomalies = []
        
        for sensor in sensors:
            # Pega os índices que terão anomalia neste sensor específico
            sensor_mask = (sensors_to_alter == sensor)
            indices_for_sensor = anomaly_indices[sensor_mask]
            
            if len(indices_for_sensor) > 0:
                mean = final_df[sensor].mean()
                std_dev = final_df[sensor].std()
                
                # Gera todos os valores anômalos para este sensor de uma vez
                anomaly_values = mean + np.random.choice([-1, 1], size=len(indices_for_sensor)) * 5 * std_dev
                
                # Aplica as anomalias no DataFrame de forma vetorizada
                final_df.loc[indices_for_sensor, sensor] = anomaly_values
                
                # Guarda os registros para o gabarito
                anomalies_info = final_df.loc[indices_for_sensor, ['timestamp', 'id_estacao']]
                anomalies_info['sensor_anomalo'] = sensor
                anomalies_info['valor_anomalo'] = anomaly_values
                ground_truth_anomalies.append(anomalies_info)
        
        anomalies_df = pd.concat(ground_truth_anomalies)
    else:
        anomalies_df = pd.DataFrame()

    print(f"Geração de dados concluída. Total de {len(final_df)} registros e {len(anomalies_df)} anomalias geradas.")
    return final_df, anomalies_df   


if __name__ == "__main__":
    # módulo para parsear argumentos
    import argparse

    # Configura o parser de argumentos da linha de comando
    parser = argparse.ArgumentParser(description="Gerador de Dados Meteorológicos Sintéticos.")
    parser.add_argument(
        "num_estacoes", 
        type=int, 
        help="O número de estações a serem simuladas."
    )
    parser.add_argument(
        "eventos_por_estacao", 
        type=int, 
        help="O número de eventos (amostras) a serem gerados por estação."
    )
    parser.add_argument(
        "percentual_anomalias", 
        type=float, 
        help="O percentual de anomalias a serem introduzidas (ex: 0.02 para 2%)."
    )
    
    # Lê os argumentos fornecidos pelo usuário
    args = parser.parse_args()

    print(f"Parâmetros recebidos: Estações={args.num_estacoes}, Eventos/Estação={args.eventos_por_estacao}, Anomalias={args.percentual_anomalias*100}%")

    start_time = time.perf_counter()

    # Gera os dados usando os argumentos da linha de comando
    dados_gerados, anomalias_reais = generate_data(
        num_stations=args.num_estacoes,
        num_events_per_station=args.eventos_por_estacao,
        anomaly_percentage=args.percentual_anomalias,
    )
    
    end_time = time.perf_counter()
    print(f"Tempo de geração dos dados: {end_time - start_time:.4f} segundos")

    # Cria o diretório de dados se não existir
    output_dir = Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Salva os DataFrames em arquivos CSV
    print("Salvando arquivos CSV...")
    start_time = time.perf_counter()
    data_path = output_dir / "dados_meteorologicos.csv"
    anomalies_path = output_dir / "anomalias_reais.csv"
    
    dados_gerados.to_csv(data_path, index=False)
    if not anomalias_reais.empty:
        anomalias_reais.to_csv(anomalies_path, index=False)
    end_time = time.perf_counter()
    print(f"Tempo de escrita dos dados: {end_time - start_time:.4f} segundos")

    total_gerado = args.num_estacoes * args.eventos_por_estacao
    print(f"\n{total_gerado} eventos foram gerados e salvos em:")
    print(f"  - Dados: {data_path}")
    print(f"  - Anomalias de referência: {anomalies_path}")
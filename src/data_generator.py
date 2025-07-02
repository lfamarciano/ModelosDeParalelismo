import pandas as pd
import numpy as np
from pathlib import Path
import datetime

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

    # Define uma lista fixa de regiões e cria o mapeamento de estações para regiões
    regions_list = ["Sudeste", "Nordeste", "Sul", "Norte", "Centro-Oeste"]
    stations_map = {
        f"STA-{i+1:03d}": regions_list[i % len(regions_list)]
        for i in range(num_stations)
    }

    all_station_data = []
    ground_truth_anomalies = []

    # Para cada estação, gera uma série temporal de dados
    for station_id, region in stations_map.items():
        # Cria uma série temporal de timestamps
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        timestamps = pd.to_datetime(np.arange(num_events_per_station), unit='m', origin=start_date)
        
        # Gera dados base realistas simulando o ciclo diário de temperatura
        time_component = np.linspace(0, 2 * np.pi, num_events_per_station)
        
        # Variação regional para diferenciar as estações
        region_offset = list(stations_map.keys()).index(station_id) * 0.5

        # Temperatura com variação senoidal e ruído
        # Baseia-se em uma temperatura média de 25°C com variação de ±8
        temp_base = 25 + 8 * np.sin(time_component) + region_offset
        temp_noise = np.random.normal(0, 0.5, num_events_per_station)
        temperature = temp_base + temp_noise

        # Umidade inversamente correlacionada com a temperatura
        humidity_base = 60 - 20 * np.sin(time_component)
        humidity_noise = np.random.normal(0, 2, num_events_per_station)
        humidity = np.clip(humidity_base + humidity_noise, 0, 100)

        # Pressão com variações menores
        pressure_base = 1012 + 5 * np.sin(time_component / 2) # Ciclo mais lento
        pressure_noise = np.random.normal(0, 1, num_events_per_station)
        pressure = pressure_base + pressure_noise

        # Monta o DataFrame para a estação atual
        station_df = pd.DataFrame({
            "timestamp": timestamps,
            "id_estacao": station_id,
            "regiao": region,
            "temperatura": temperature,
            "umidade": humidity,
            "pressao": pressure
        })
        all_station_data.append(station_df)

    # Concatena os dados de todas as estações e ordena pelo tempo
    final_df = pd.concat(all_station_data).sort_values(by="timestamp").reset_index(drop=True)

    # Introduz anomalias
    num_anomalies = int(len(final_df) * anomaly_percentage)
    if num_anomalies > 0:
        anomaly_indices = np.random.choice(final_df.index, size=num_anomalies, replace=False)
        sensors = ["temperatura", "umidade", "pressao"]

        for idx in anomaly_indices:
            sensor_to_alter = np.random.choice(sensors)
            
            mean = final_df[sensor_to_alter].mean()
            std_dev = final_df[sensor_to_alter].std()
            anomaly_value = mean + np.random.choice([-1, 1]) * 5 * std_dev
            
            final_df.loc[idx, sensor_to_alter] = anomaly_value
            
            ground_truth_anomalies.append({
                "timestamp": final_df.loc[idx, "timestamp"],
                "id_estacao": final_df.loc[idx, "id_estacao"],
                "sensor_anomalo": sensor_to_alter,
                "valor_anomalo": anomaly_value
            })
    
    anomalies_df = pd.DataFrame(ground_truth_anomalies)

    total_eventos = num_stations * num_events_per_station
    print(f"Geração de dados concluída. Total de {total_eventos} registros e {len(anomalies_df)} anomalias geradas.")
    
    final_df = final_df[["timestamp", "id_estacao", "regiao", "temperatura", "umidade", "pressao"]]
    
    return final_df, anomalies_df


if __name__ == "__main__":
    # PARÂMETROS DO EXPERIMENTO
    NUM_ESTACOES = 12
    EVENTOS_POR_ESTACAO = 60 
    PERCENTUAL_ANOMALIAS = 0.02 # 2%

    # Gera os dados
    dados_gerados, anomalias_reais = generate_data(
        num_stations=NUM_ESTACOES,
        num_events_per_station=EVENTOS_POR_ESTACAO,
        anomaly_percentage=PERCENTUAL_ANOMALIAS,
        start_date_str="2025-07-01"
    )

    # Cria o diretório de dados se não existir
    output_dir = Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Salva os DataFrames em arquivos CSV
    data_path = output_dir / "dados_meteorologicos.csv"
    anomalies_path = output_dir / "anomalias_reais.csv"
    
    dados_gerados.to_csv(data_path, index=False)
    anomalias_reais.to_csv(anomalies_path, index=False)

    total_gerado = NUM_ESTACOES * EVENTOS_POR_ESTACAO
    print(f"\n{total_gerado} eventos foram gerados e salvos em:")
    print(f"  - Dados: {data_path}")
    print(f"  - Anomalias de referência: {anomalies_path}")
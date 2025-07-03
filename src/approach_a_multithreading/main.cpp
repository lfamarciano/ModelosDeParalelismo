// g++ -std=c++17 -O3 -pthread src/approach_a_multithreading/main.cpp -o processador_cpp
// ./processador_cpp data/dados_meteorologicos.csv data/resultado_cpp.json 4
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include <numeric>
#include <cmath>
#include <map>
#include <set>
#include <algorithm>
#include <iomanip> 
#include <fstream> 

// Nossas bibliotecas de terceiros
#include "include/csv.h"
#include "include/json.hpp"

// Usando nlohmann::json para facilitar
using json = nlohmann::json;

// Estrutura para manter uma linha de dados do CSV
struct DataRow {
    std::chrono::system_clock::time_point timestamp;
    std::string station_id;
    std::string region;
    double temperature;
    double humidity;
    double pressure;
};

// Estrutura para os resultados das métricas
struct StationMetrics {
    std::map<std::string, double> anomaly_percentages;
    long concurrent_anomaly_periods = 0;
};

// Função para identificar anomalias (média +/- 3 * desvio padrão)
bool is_anomaly(double value, double mean, double std_dev) {
    return value < mean - 3 * std_dev || value > mean + 3 * std_dev;
}

// Função que será executada por cada thread
StationMetrics process_station_partition(const std::vector<DataRow>& all_data, const std::string& station_id) {
    StationMetrics metrics;
    std::vector<DataRow> station_data;

    // 1. Filtrar dados apenas para esta estação
    for (const auto& row : all_data) {
        if (row.station_id == station_id) {
            station_data.push_back(row);
        }
    }

    if (station_data.empty()) {
        return metrics;
    }
    
    // 2. Calcular Média e Desvio Padrão para a estação
    double temp_sum = 0, hum_sum = 0, press_sum = 0;
    for(const auto& row : station_data) {
        temp_sum += row.temperature;
        hum_sum += row.humidity;
        press_sum += row.pressure;
    }
    double temp_mean = temp_sum / station_data.size();
    double hum_mean = hum_sum / station_data.size();
    double press_mean = press_sum / station_data.size();

    double temp_sq_sum = 0, hum_sq_sum = 0, press_sq_sum = 0;
     for(const auto& row : station_data) {
        temp_sq_sum += (row.temperature - temp_mean) * (row.temperature - temp_mean);
        hum_sq_sum += (row.humidity - hum_mean) * (row.humidity - hum_mean);
        press_sq_sum += (row.pressure - press_mean) * (row.pressure - press_mean);
    }
    double temp_std = std::sqrt(temp_sq_sum / station_data.size());
    double hum_std = std::sqrt(hum_sq_sum / station_data.size());
    double press_std = std::sqrt(press_sq_sum / station_data.size());

    // 3. Calcular Métricas 1 e 3
    long temp_anomalies = 0, hum_anomalies = 0, press_anomalies = 0;
    // Mapa para Métrica 3: agrupa anomalias por janelas de 10 minutos
    std::map<long, std::set<std::string>> concurrent_map;

    for(const auto& row : station_data) {
        bool temp_is_a = is_anomaly(row.temperature, temp_mean, temp_std);
        bool hum_is_a = is_anomaly(row.humidity, hum_mean, hum_std);
        bool press_is_a = is_anomaly(row.pressure, press_mean, press_std);

        if(temp_is_a) temp_anomalies++;
        if(hum_is_a) hum_anomalies++;
        if(press_is_a) press_anomalies++;
        
        // Lógica para Métrica 3
        if (temp_is_a || hum_is_a || press_is_a) {
            long time_bucket = std::chrono::duration_cast<std::chrono::minutes>(row.timestamp.time_since_epoch()).count() / 10;
            if(temp_is_a) concurrent_map[time_bucket].insert("temperatura");
            if(hum_is_a) concurrent_map[time_bucket].insert("umidade");
            if(press_is_a) concurrent_map[time_bucket].insert("pressao");
        }
    }
    
    // Finaliza Métrica 1
    metrics.anomaly_percentages["temperatura"] = (static_cast<double>(temp_anomalies) / station_data.size()) * 100.0;
    metrics.anomaly_percentages["umidade"] = (static_cast<double>(hum_anomalies) / station_data.size()) * 100.0;
    metrics.anomaly_percentages["pressao"] = (static_cast<double>(press_anomalies) / station_data.size()) * 100.0;

    // Finaliza Métrica 3
    for(const auto& pair : concurrent_map) {
        if (pair.second.size() > 1) {
            metrics.concurrent_anomaly_periods++;
        }
    }
    
    // Métrica 2 (Média Móvel) é mais complexa de agregar entre threads, 
    // então para este exemplo, vamos deixá-la de fora da implementação C++
    // para focar na comparação do paralelismo de tarefas que são "por estação".
    // A implementação completa seria feita em Spark, que lida com isso nativamente.

    return metrics;
}


int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Uso: " << argv[0] << " <arquivo_csv> <arquivo_json_saida> <num_threads>" << std::endl;
        return 1;
    }

    std::string csv_path = argv[1];
    std::string json_path = argv[2];
    int num_threads = std::stoi(argv[3]);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Carregar dados do CSV
    std::vector<DataRow> data;
    std::set<std::string> station_ids;
    try {
        io::CSVReader<6> in(csv_path);
        in.read_header(io::ignore_extra_column, "timestamp", "id_estacao", "regiao", "temperatura", "umidade", "pressao");
        
        std::string ts_str, station, region;
        double temp, hum, press;
        while (in.read_row(ts_str, station, region, temp, hum, press)) {
            DataRow row;
            std::tm tm = {};
            // Formato do timestamp do pandas: 2025-07-01 00:00:00
            std::stringstream ss(ts_str);
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
            row.timestamp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
            row.station_id = station;
            row.region = region;
            row.temperature = temp;
            row.humidity = hum;
            row.pressure = press;
            data.push_back(row);
            station_ids.insert(station);
        }
    } catch (const std::exception& e) {
        std::cerr << "Erro ao ler o CSV: " << e.what() << std::endl;
        return 1;
    }

    // Dividir as estações entre as threads
    std::vector<std::thread> threads;
    std::vector<std::string> stations(station_ids.begin(), station_ids.end());
    std::vector<StationMetrics> results(stations.size());
    
    for (int i = 0; i < stations.size(); ++i) {
        // Para simplificar, criamos uma thread por estação. 
        // Em um cenário com mais estações que threads, criaríamos um pool.
        threads.emplace_back([&, i]() {
            results[i] = process_station_partition(data, stations[i]);
        });
    }

    for (auto& t : threads) {
        if(t.joinable()) t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end_time - start_time;

    // Agregar resultados para o JSON
    json final_json;
    final_json["tempo_execucao_ms"] = elapsed.count();
    
    for(size_t i = 0; i < stations.size(); ++i) {
        final_json["resultados"][stations[i]]["percentual_anomalias"] = results[i].anomaly_percentages;
        final_json["resultados"][stations[i]]["periodos_concorrentes"] = results[i].concurrent_anomaly_periods;
    }
    
    // Salvar JSON
    std::ofstream o(json_path);
    o << std::setw(4) << final_json << std::endl;

    std::cout << "Processamento C++ concluído em " << elapsed.count() << " ms." << std::endl;

    return 0;
}
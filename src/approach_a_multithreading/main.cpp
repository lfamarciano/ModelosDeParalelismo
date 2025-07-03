// g++ -std=c++17 -O3 -pthread src/approach_a_multithreading/main.cpp -o processador_cpp
// ./processador_cpp data/dados_meteorologicos.csv data/resultado_cpp.json 8
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <chrono>
#include <numeric>
#include <cmath>
#include <map>
#include <set>
#include <algorithm>
#include <iomanip>
#include <fstream>
#include <deque>

#include "include/csv.h"
#include "include/json.hpp"

using json = nlohmann::json;

// Estrutura para os dados brutos
struct DataRow {
    long long timestamp_ms; 
    std::string station_id;
    std::string region;
    double temperature;
    double humidity;
    double pressure;
    // Marcadores para anomalias
    bool temp_is_anomaly = false;
    bool hum_is_anomaly = false;
    bool press_is_anomaly = false;
};

// Estruturas para os resultados
struct StationMetrics {
    std::map<std::string, double> anomaly_percentages;
    long concurrent_anomaly_periods = 0;
};

// Serialização da struct criada para JSON
void to_json(json& j, const StationMetrics& sm) {
    j = json{
        {"percentual_anomalias", sm.anomaly_percentages},
        {"periodos_concorrentes", sm.concurrent_anomaly_periods}
    };
}

// Mutex para proteger o acesso à fila de tarefas e ao mapa de resultados
std::mutex tasks_mutex;
std::mutex results_mutex;

// Função para identificar anomalias
bool is_anomaly(double value, double mean, double std_dev) {
    return value < mean - 3 * std_dev || value > mean + 3 * std_dev;
}

// Lógica que cada thread do pool executa
void worker_logic(
    std::vector<std::string>& station_tasks,
    const std::map<std::string, std::vector<int>>& station_data_indices,
    std::vector<DataRow>& all_data,
    std::map<std::string, StationMetrics>& results
) {
    while (true) {
        std::string current_station_id;

        // Pega uma tarefa da fila de forma segura
        {
            std::lock_guard<std::mutex> lock(tasks_mutex);
            if (station_tasks.empty()) {
                break; // Fila vazia, a thread termina
            }
            current_station_id = station_tasks.back();
            station_tasks.pop_back();
        }

        // Se não houver dados para a estação, pula para a próxima
        const auto& indices = station_data_indices.at(current_station_id);
        if (indices.empty()) continue;

        // Calcular Média e Desvio Padrão
        double temp_sum = 0, hum_sum = 0, press_sum = 0;
        for (int idx : indices) {
            temp_sum += all_data[idx].temperature;
            hum_sum += all_data[idx].humidity;
            press_sum += all_data[idx].pressure;
        }
        double temp_mean = temp_sum / indices.size();
        double hum_mean = hum_sum / indices.size();
        double press_mean = press_sum / indices.size();
        
        double temp_sq_sum = 0, hum_sq_sum = 0, press_sq_sum = 0;
        for (int idx : indices) {
            temp_sq_sum += (all_data[idx].temperature - temp_mean) * (all_data[idx].temperature - temp_mean);
            hum_sq_sum += (all_data[idx].humidity - hum_mean) * (all_data[idx].humidity - hum_mean);
            press_sq_sum += (all_data[idx].pressure - press_mean) * (all_data[idx].pressure - press_mean);
        }
        double temp_std = std::sqrt(temp_sq_sum / indices.size());
        double hum_std = std::sqrt(hum_sq_sum / indices.size());
        double press_std = std::sqrt(press_sq_sum / indices.size());

        // Calcular Métricas 1 e 3 e marcar anomalias no vetor principal de dados
        long temp_anomalies = 0, hum_anomalies = 0, press_anomalies = 0;
        std::map<long, std::set<std::string>> concurrent_map;

        for (int idx : indices) {
            bool temp_is_a = is_anomaly(all_data[idx].temperature, temp_mean, temp_std);
            bool hum_is_a = is_anomaly(all_data[idx].humidity, hum_mean, hum_std);
            bool press_is_a = is_anomaly(all_data[idx].pressure, press_mean, press_std);

            if (temp_is_a) { temp_anomalies++; all_data[idx].temp_is_anomaly = true; }
            if (hum_is_a) { hum_anomalies++; all_data[idx].hum_is_anomaly = true; }
            if (press_is_a) { press_anomalies++; all_data[idx].press_is_anomaly = true; }

            if (temp_is_a || hum_is_a || press_is_a) {
                long time_bucket = all_data[idx].timestamp_ms / (1000 * 60 * 10); // Bucket de 10 min
                if (temp_is_a) concurrent_map[time_bucket].insert("temperatura");
                if (hum_is_a) concurrent_map[time_bucket].insert("umidade");
                if (press_is_a) concurrent_map[time_bucket].insert("pressao");
            }
        }

        StationMetrics station_metrics;
        station_metrics.anomaly_percentages["temperatura"] = (static_cast<double>(temp_anomalies) / indices.size()) * 100.0;
        station_metrics.anomaly_percentages["umidade"] = (static_cast<double>(hum_anomalies) / indices.size()) * 100.0;
        station_metrics.anomaly_percentages["pressao"] = (static_cast<double>(press_anomalies) / indices.size()) * 100.0;

        for (const auto& pair : concurrent_map) {
            if (pair.second.size() > 1) {
                station_metrics.concurrent_anomaly_periods++;
            }
        }

        // Adiciona o resultado ao mapa de resultados de forma segura
        {
            std::lock_guard<std::mutex> lock(results_mutex);
            results[current_station_id] = station_metrics;
        }
    }
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

    // Carrega dados do CSV para a memória
    std::vector<DataRow> all_data;
    std::map<std::string, std::vector<int>> station_data_indices;
    try {
        io::CSVReader<6> in(csv_path);
        in.read_header(io::ignore_extra_column, "timestamp", "id_estacao", "regiao", "temperatura", "umidade", "pressao");
        
        int index = 0;
        std::string ts_str, station, region;
        double temp, hum, press;
        while (in.read_row(ts_str, station, region, temp, hum, press)) {
            DataRow row;
            std::tm tm = {};
            std::stringstream ss(ts_str);
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
            auto time_point = std::chrono::system_clock::from_time_t(std::mktime(&tm));
            row.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch()).count();
            
            row.station_id = station;
            row.region = region;
            row.temperature = temp;
            row.humidity = hum;
            row.pressure = press;
            all_data.push_back(row);
            station_data_indices[station].push_back(index++);
        }
    } catch (const std::exception& e) {
        std::cerr << "Erro ao ler o CSV: " << e.what() << std::endl;
        return 1;
    }

    // Prepara a fila de tarefas e o mapa de resultados
    std::vector<std::string> station_tasks;
    for(const auto& pair : station_data_indices) {
        station_tasks.push_back(pair.first);
    }
    std::map<std::string, StationMetrics> results;

    // Lança o pool de threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker_logic, std::ref(station_tasks), std::cref(station_data_indices), std::ref(all_data), std::ref(results));
    }
    for (auto& t : threads) {
        t.join();
    }

    // Cálculo Sequencial da Métrica 2 
    std::vector<DataRow> clean_data;
    for (const auto& row : all_data) {
        if (!row.temp_is_anomaly && !row.hum_is_anomaly && !row.press_is_anomaly) {
            clean_data.push_back(row);
        }
    }
    // Ordena os dados limpos por tempo, crucial para a média móvel
    std::sort(clean_data.begin(), clean_data.end(), [](const DataRow& a, const DataRow& b) {
        return a.timestamp_ms < b.timestamp_ms;
    });

    std::map<std::string, std::vector<DataRow>> data_by_region;
    for (const auto& row : clean_data) {
        data_by_region[row.region].push_back(row);
    }
    
    json moving_averages_json;
    const int window_size = 10;
    for (auto const& [region, region_data] : data_by_region) {
        std::deque<double> temp_window, hum_window, press_window;
        double temp_sum = 0, hum_sum = 0, press_sum = 0;

        for (const auto& row : region_data) {
            temp_window.push_back(row.temperature);
            temp_sum += row.temperature;
            hum_window.push_back(row.humidity);
            hum_sum += row.humidity;
            press_window.push_back(row.pressure);
            press_sum += row.pressure;

            if (temp_window.size() > window_size) {
                temp_sum -= temp_window.front(); temp_window.pop_front();
                hum_sum -= hum_window.front(); hum_window.pop_front();
                press_sum -= press_window.front(); press_window.pop_front();
            }

            json mov_avg_row;
            mov_avg_row["timestamp"] = row.timestamp_ms; // Poderia formatar de volta para string
            mov_avg_row["regiao"] = region;
            mov_avg_row["temperatura_mov_avg"] = temp_sum / temp_window.size();
            mov_avg_row["umidade_mov_avg"] = hum_sum / hum_window.size();
            mov_avg_row["pressao_mov_avg"] = press_sum / press_window.size();
            moving_averages_json[region].push_back(mov_avg_row);
        }
    }


    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end_time - start_time;

    // Agrega todos os resultados no JSON final
    json final_json;
    final_json["tempo_execucao_ms"] = elapsed.count();
    final_json["resultados_por_estacao"] = results;
    // Omitindo a média móvel do JSON para não poluir, mas ela foi calculada
    // final_json["media_movel_por_regiao"] = moving_averages_json;

    std::ofstream o(json_path);
    o << std::setw(4) << final_json << std::endl;

    std::cout << "Processamento C++ (Thread Pool) concluído em " << elapsed.count() << " ms." << std::endl;

    return 0;
}
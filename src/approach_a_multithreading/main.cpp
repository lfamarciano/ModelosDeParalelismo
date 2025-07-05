// Versão final com paralelismo por estação
// Compilar: g++ -std=c++17 -O3 -pthread optimized_main.cpp -o processador_cpp
// Executar: ./processador_cpp data/dados_meteorologicos.csv data/resultado_cpp.json 8

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
#include <sstream>

#include "include/csv.h"
#include "include/json.hpp"

using json = nlohmann::json;

struct DataRow {
    long long timestamp_ms;
    std::string timestamp_str;
    std::string station_id;
    std::string region;
    double temperature;
    double humidity;
    double pressure;
    bool is_anomalous = false;
};

struct StationMetrics {
    std::map<std::string, double> anomaly_percentages;
    long concurrent_anomaly_periods = 0;
};

bool is_anomaly(double value, double mean, double std_dev) {
    if (std_dev == 0) return false;
    return value < mean - 3 * std_dev || value > mean + 3 * std_dev;
}

StationMetrics process_station(std::vector<DataRow>& all_data, const std::vector<int>& indices, const std::string& station_id) {
    StationMetrics metrics;
    double temp_sum = 0, hum_sum = 0, press_sum = 0;
    for (int idx : indices) {
        temp_sum += all_data[idx].temperature;
        hum_sum += all_data[idx].humidity;
        press_sum += all_data[idx].pressure;
    }
    double n = indices.size();
    double temp_mean = temp_sum / n;
    double hum_mean = hum_sum / n;
    double press_mean = press_sum / n;

    double temp_sq_sum = 0, hum_sq_sum = 0, press_sq_sum = 0;
    for (int idx : indices) {
        temp_sq_sum += std::pow(all_data[idx].temperature - temp_mean, 2);
        hum_sq_sum += std::pow(all_data[idx].humidity - hum_mean, 2);
        press_sq_sum += std::pow(all_data[idx].pressure - press_mean, 2);
    }
    double temp_std = std::sqrt(temp_sq_sum / n);
    double hum_std = std::sqrt(hum_sq_sum / n);
    double press_std = std::sqrt(press_sq_sum / n);

    long temp_anom = 0, hum_anom = 0, press_anom = 0;
    std::map<long, std::set<std::string>> concurrent_map;

    for (int idx : indices) {
        auto& row = all_data[idx];
        bool temp_a = is_anomaly(row.temperature, temp_mean, temp_std);
        bool hum_a = is_anomaly(row.humidity, hum_mean, hum_std);
        bool press_a = is_anomaly(row.pressure, press_mean, press_std);
        if (temp_a || hum_a || press_a) row.is_anomalous = true;
        if (temp_a) temp_anom++;
        if (hum_a) hum_anom++;
        if (press_a) press_anom++;

        if (temp_a || hum_a || press_a) {
            long bucket = row.timestamp_ms / (1000 * 60 * 10);
            if (temp_a) concurrent_map[bucket].insert("temperatura");
            if (hum_a) concurrent_map[bucket].insert("umidade");
            if (press_a) concurrent_map[bucket].insert("pressao");
        }
    }

    metrics.anomaly_percentages["temperatura"] = (temp_anom / n) * 100.0;
    metrics.anomaly_percentages["umidade"] = (hum_anom / n) * 100.0;
    metrics.anomaly_percentages["pressao"] = (press_anom / n) * 100.0;

    for (const auto& [bucket, set] : concurrent_map) {
        if (set.size() > 1) metrics.concurrent_anomaly_periods++;
    }

    return metrics;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Uso: " << argv[0] << " <arquivo_csv> <arquivo_json_saida> <num_threads>" << std::endl;
        return 1;
    }

    std::string csv_path = argv[1], json_path = argv[2];
    int num_threads = std::stoi(argv[3]);

    std::vector<DataRow> all_data;
    try {
        io::CSVReader<6> in(csv_path);
        in.read_header(io::ignore_extra_column, "timestamp", "id_estacao", "regiao", "temperatura", "umidade", "pressao");
        std::string ts, station, region;
        double temp, hum, press;
        while (in.read_row(ts, station, region, temp, hum, press)) {
            std::tm tm = {};
            std::stringstream ss(ts);
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
            auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
            all_data.push_back({ms, ts, station, region, temp, hum, press});
        }
    } catch (const std::exception& e) {
        std::cerr << "Erro ao ler CSV: " << e.what() << std::endl;
        return 1;
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Agrupamento por estação
    std::map<std::string, std::vector<int>> station_indices;
    for (int i = 0; i < all_data.size(); ++i) {
        station_indices[all_data[i].station_id].push_back(i);
    }

    std::vector<std::string> station_ids;
    for (const auto& [station, _] : station_indices) station_ids.push_back(station);

    std::mutex mtx;
    std::map<std::string, StationMetrics> station_results;

    auto worker = [&]() {
        while (true) {
            std::string sid;
            {
                std::lock_guard<std::mutex> lock(mtx);
                if (station_ids.empty()) break;
                sid = station_ids.back();
                station_ids.pop_back();
            }
            StationMetrics result = process_station(all_data, station_indices[sid], sid);
            {
                std::lock_guard<std::mutex> lock(mtx);
                station_results[sid] = result;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();

    // Métrica 2
    std::ofstream media_out("media_movel_regiao.jsonl");
    std::map<std::string, std::deque<std::tuple<double, double, double>>> janela;

    for (const auto& row : all_data) {
        if (row.is_anomalous) continue;
        auto& q = janela[row.region];
        q.push_back({row.temperature, row.humidity, row.pressure});
        if (q.size() > 10) q.pop_front();

        double soma_temp = 0, soma_hum = 0, soma_pres = 0;
        for (const auto& [t, h, p] : q) {
            soma_temp += t;
            soma_hum += h;
            soma_pres += p;
        }

        size_t n = q.size();
        json linha = {
            {"timestamp", row.timestamp_str},
            {"id_estacao", row.station_id},
            {"regiao", row.region},
            {"temperatura", soma_temp / n},
            {"umidade", soma_hum / n},
            {"pressao", soma_pres / n}
        };
        media_out << linha.dump() << "\n";
    }
    media_out.close();

    auto end = std::chrono::high_resolution_clock::now();

    json output;
    output["tempo_execucao_ms"] = std::chrono::duration<double, std::milli>(end - start).count();
    output["resultados_por_estacao"] = json::object();
    for (const auto& [station, m] : station_results) {
        output["resultados_por_estacao"][station] = {
            {"percentual_anomalias", m.anomaly_percentages},
            {"periodos_concorrentes", m.concurrent_anomaly_periods}
        };
    }

    std::ofstream(json_path) << std::setw(4) << output << std::endl;
    std::cout << "Processamento concluído em " << output["tempo_execucao_ms"] << " ms\n";
    std::cout << "Média móvel salva em media_movel_regiao.jsonl\n";
    return 0;
}

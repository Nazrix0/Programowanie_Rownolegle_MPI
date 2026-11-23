#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mpi.h>
#include <string>
#include <vector>

#include "sha256.hpp"

namespace fs = std::filesystem;

// Tagi (etykiety) rozróżniające typ wiadomości w MPI
constexpr int TAG_WORK = 1; // Surowe bajty pliku do policzenia
constexpr int TAG_HASH = 2; // Gotowy 64-znakowy skrót SHA-256
constexpr int TAG_STOP = 3; // Sygnał zakończenia pracy dla procesów roboczych

struct FileTask {
    std::string full_path;
    std::string rel_path;
    std::string sha256_hash;
};

// Liczenie SHA-256 z bufora bajtów otrzymanego przez MPI (używa klasy SHA256 bez zmian)
std::string hash_buffer(const std::vector<uint8_t>& buf) {
    SHA256 ctx;
    if (!buf.empty()) {
        ctx.update(buf.data(), buf.size());
    }
    return ctx.final_hex();
}

// Wczytanie całego pliku do wektora bajtów (wykonuje tylko proces główny rank 0)
std::vector<uint8_t> read_file_bytes(const std::string& filepath) {
    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) return {};
    std::streamsize size = file.tellg();
    if (size <= 0) return {};
    file.seekg(0, std::ios::beg);
    std::vector<uint8_t> buffer(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(buffer.data()), size);
    return buffer;
}

// Budowa Drzewa Merkle'a z posortowanych skrótów wszystkich plików
std::string build_merkle_tree(std::vector<std::string> current_level) {
    if (current_level.empty()) {
        return SHA256::hash_string("");
    }

    while (current_level.size() > 1) {
        if (current_level.size() % 2 != 0) {
            current_level.push_back(current_level.back());
        }

        size_t next_size = current_level.size() / 2;
        std::vector<std::string> next_level(next_size);

        for (size_t i = 0; i < next_size; ++i) {
            next_level[i] = SHA256::hash_string(current_level[2 * i] + current_level[2 * i + 1]);
        }

        current_level = std::move(next_level);
    }

    return current_level[0];
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int num_procs = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    std::string target_dir = "";
    std::string mode = "nonblocking";
    std::string output_manifest = "";

    // Parsowanie flag z linii poleceń
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-d" || arg == "--dir") && i + 1 < argc) {
            target_dir = argv[++i];
        } else if ((arg == "-m" || arg == "--mode") && i + 1 < argc) {
            mode = argv[++i];
        } else if ((arg == "-o" || arg == "--output") && i + 1 < argc) {
            output_manifest = argv[++i];
        }
    }

    if (target_dir.empty() || !fs::exists(target_dir) || !fs::is_directory(target_dir)) {
        if (rank == 0) {
            std::cerr << "Uzycie: mpirun -np <procesy> " << argv[0]
                      << " -d <katalog> [-m <blocking|nonblocking>] [-o <manifest>]\n";
        }
        MPI_Finalize();
        return 1;
    }

    if (mode != "blocking" && mode != "nonblocking") {
        if (rank == 0) {
            std::cerr << "Blad: Flaga -m musi miec wartosc 'blocking' lub 'nonblocking'.\n";
        }
        MPI_Finalize();
        return 1;
    }

    // 1. Proces główny (rank 0) pobiera wszystkie pliki z katalogu i sortuje je alfabetycznie
    std::vector<FileTask> tasks;
    if (rank == 0) {
        for (const auto& entry : fs::recursive_directory_iterator(target_dir)) {
            if (entry.is_regular_file()) {
                FileTask task;
                task.full_path = entry.path().string();
                task.rel_path = fs::relative(entry.path(), target_dir).generic_string();
                task.sha256_hash.resize(64, '0');
                tasks.push_back(std::move(task));
            }
        }
        std::sort(tasks.begin(), tasks.end(), [](const FileTask& a, const FileTask& b) {
            return a.rel_path < b.rel_path;
        });
    }

    // Wspólna linia startu pomiaru dla wszystkich procesów
    MPI_Barrier(MPI_COMM_WORLD);
    double local_comp_time = 0.0;
    double local_comm_time = 0.0;

    if (num_procs == 1) {
        // Wersja bazowa (1 proces): liczy wszystkie pliki lokalnie bez MPI
        double t0 = MPI_Wtime();
        for (size_t i = 0; i < tasks.size(); ++i) {
            tasks[i].sha256_hash = SHA256::hash_file(tasks[i].full_path);
        }
        local_comp_time = MPI_Wtime() - t0;
    } else if (rank == 0) {
        // PROCES GŁÓWNY (rank 0): rozsyła pliki do innych procesów ORAZ sam liczy swoją część
        const size_t n_files = tasks.size();
        const int num_workers = num_procs - 1;

        if (mode == "blocking") {
            // =========================================================================
            // TRYB 1: BLOCKING (MPI_Send / MPI_Recv) — przetwarzanie sztywnymi falami
            // =========================================================================
            size_t next_idx = 0;
            while (next_idx < n_files) {
                int active_in_wave = 0;
                std::vector<size_t> wave_file_idx(num_workers + 1, 0);

                // Krok A: Wysłanie po 1 pliku do każdego procesu roboczego (blokujące MPI_Send)
                for (int w = 1; w <= num_workers && next_idx < n_files; ++w, ++next_idx) {
                    std::vector<uint8_t> buf = read_file_bytes(tasks[next_idx].full_path);
                    wave_file_idx[w] = next_idx;
                    active_in_wave++;

                    double c0 = MPI_Wtime();
                    MPI_Send(buf.data(), static_cast<int>(buf.size()), MPI_BYTE, w, TAG_WORK, MPI_COMM_WORLD);
                    local_comm_time += (MPI_Wtime() - c0);
                }

                // Krok B: Proces główny (rank 0) również bierze 1 plik i liczy go równolegle
                if (next_idx < n_files) {
                    size_t my_idx = next_idx++;
                    std::vector<uint8_t> my_buf = read_file_bytes(tasks[my_idx].full_path);

                    double t0 = MPI_Wtime();
                    tasks[my_idx].sha256_hash = hash_buffer(my_buf);
                    local_comp_time += (MPI_Wtime() - t0);
                }

                // Krok C: Odbiór wyników ze wszystkich procesów z fali w sztywnej kolejności
                for (int w = 1; w <= active_in_wave; ++w) {
                    size_t f_idx = wave_file_idx[w];
                    double c0 = MPI_Wtime();
                    MPI_Recv(&tasks[f_idx].sha256_hash[0], 64, MPI_CHAR, w, TAG_HASH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    local_comm_time += (MPI_Wtime() - c0);
                }
            }

            // Zakończenie pracy wszystkich procesów roboczych
            for (int w = 1; w <= num_workers; ++w) {
                double c0 = MPI_Wtime();
                MPI_Send(nullptr, 0, MPI_BYTE, w, TAG_STOP, MPI_COMM_WORLD);
                local_comm_time += (MPI_Wtime() - c0);
            }
        } else {
            // =========================================================================
            // TRYB 2: NONBLOCKING (MPI_Isend / MPI_Irecv + MPI_Test) — nakładanie pracy
            // =========================================================================
            std::vector<std::vector<uint8_t>> worker_bufs(num_workers + 1);
            std::vector<MPI_Request> send_reqs(num_workers + 1, MPI_REQUEST_NULL);
            std::vector<MPI_Request> recv_reqs(num_workers + 1, MPI_REQUEST_NULL);
            std::vector<bool> worker_active(num_workers + 1, false);

            size_t next_idx = 0;
            int active_count = 0;

            // Funkcja zlecająca w tle (bez czekania) kolejny plik wybranemu procesowi
            auto dispatch_to_worker = [&](int w) {
                if (send_reqs[w] != MPI_REQUEST_NULL) {
                    double c0 = MPI_Wtime();
                    MPI_Wait(&send_reqs[w], MPI_STATUS_IGNORE);
                    local_comm_time += (MPI_Wtime() - c0);
                }
                if (next_idx < n_files) {
                    size_t f_idx = next_idx++;
                    worker_bufs[w] = read_file_bytes(tasks[f_idx].full_path);

                    double c0 = MPI_Wtime();
                    MPI_Isend(worker_bufs[w].data(), static_cast<int>(worker_bufs[w].size()), MPI_BYTE,
                              w, TAG_WORK, MPI_COMM_WORLD, &send_reqs[w]);
                    MPI_Irecv(&tasks[f_idx].sha256_hash[0], 64, MPI_CHAR,
                              w, TAG_HASH, MPI_COMM_WORLD, &recv_reqs[w]);
                    local_comm_time += (MPI_Wtime() - c0);

                    worker_active[w] = true;
                    active_count++;
                } else {
                    worker_active[w] = false;
                }
            };

            // Funkcja sprawdzająca w tle, czy wysłane paczki dotarły i czy któryś proces już skończył
            auto poll_workers = [&]() {
                for (int w = 1; w <= num_workers; ++w) {
                    if (send_reqs[w] != MPI_REQUEST_NULL) {
                        int sflag = 0;
                        double c0 = MPI_Wtime();
                        MPI_Test(&send_reqs[w], &sflag, MPI_STATUS_IGNORE);
                        local_comm_time += (MPI_Wtime() - c0);
                    }
                    if (worker_active[w]) {
                        int rflag = 0;
                        double c0 = MPI_Wtime();
                        MPI_Test(&recv_reqs[w], &rflag, MPI_STATUS_IGNORE);
                        local_comm_time += (MPI_Wtime() - c0);

                        if (rflag) {
                            worker_active[w] = false;
                            active_count--;
                            dispatch_to_worker(w);
                        }
                    }
                }
            };

            // Rozesłanie początkowej puli plików do wszystkich procesów naraz w tle
            for (int w = 1; w <= num_workers; ++w) {
                dispatch_to_worker(w);
            }
            poll_workers();

            // Główna pętla: rank 0 liczy własny plik kawałkami po 512 KB, a w przerwach obsługuje MPI w tle
            constexpr size_t CHUNK_SIZE = 512 * 1024;
            while (next_idx < n_files || active_count > 0) {
                poll_workers();

                if (next_idx < n_files) {
                    size_t my_idx = next_idx++;
                    std::vector<uint8_t> my_buf = read_file_bytes(tasks[my_idx].full_path);

                    SHA256 ctx;
                    for (size_t offset = 0; offset < my_buf.size(); offset += CHUNK_SIZE) {
                        size_t len = std::min(CHUNK_SIZE, my_buf.size() - offset);
                        double t0 = MPI_Wtime();
                        ctx.update(my_buf.data() + offset, len);
                        local_comp_time += (MPI_Wtime() - t0);

                        // Obsługa komunikacji w tle bez blokowania innych procesów
                        poll_workers();
                    }
                    double t_fin = MPI_Wtime();
                    tasks[my_idx].sha256_hash = ctx.final_hex();
                    local_comp_time += (MPI_Wtime() - t_fin);
                }
            }

            // Domknięcie ewentualnych uchwytów wysyłek i wysłanie sygnału zakończenia pracy
            for (int w = 1; w <= num_workers; ++w) {
                double c0 = MPI_Wtime();
                if (send_reqs[w] != MPI_REQUEST_NULL) {
                    MPI_Wait(&send_reqs[w], MPI_STATUS_IGNORE);
                }
                MPI_Send(nullptr, 0, MPI_BYTE, w, TAG_STOP, MPI_COMM_WORLD);
                local_comm_time += (MPI_Wtime() - c0);
            }
        }
    } else {
        // PROCESY ROBOCZE (rank != 0): odbierają plik, liczą SHA-256 i odsyłają 64-bajtowy skrót
        while (true) {
            MPI_Status status;
            double c0 = MPI_Wtime();
            MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

            if (status.MPI_TAG == TAG_STOP) {
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, TAG_STOP, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                local_comm_time += (MPI_Wtime() - c0);
                break;
            }

            int count = 0;
            MPI_Get_count(&status, MPI_BYTE, &count);
            std::vector<uint8_t> buf(static_cast<size_t>(count));

            if (mode == "blocking") {
                MPI_Recv(buf.data(), count, MPI_BYTE, 0, TAG_WORK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            } else {
                MPI_Request r_req;
                MPI_Irecv(buf.data(), count, MPI_BYTE, 0, TAG_WORK, MPI_COMM_WORLD, &r_req);
                MPI_Wait(&r_req, MPI_STATUS_IGNORE);
            }
            local_comm_time += (MPI_Wtime() - c0);

            // Czyste obliczenia SHA-256
            double t0 = MPI_Wtime();
            std::string hash = hash_buffer(buf);
            local_comp_time += (MPI_Wtime() - t0);

            // Odesłanie wyliczonego skrótu do procesu głównego
            double c1 = MPI_Wtime();
            if (mode == "blocking") {
                MPI_Send(hash.data(), 64, MPI_CHAR, 0, TAG_HASH, MPI_COMM_WORLD);
            } else {
                MPI_Request s_req;
                MPI_Isend(hash.data(), 64, MPI_CHAR, 0, TAG_HASH, MPI_COMM_WORLD, &s_req);
                MPI_Wait(&s_req, MPI_STATUS_IGNORE);
            }
            local_comm_time += (MPI_Wtime() - c1);
        }
    }

    // Zsumowanie czasów obliczeń oraz komunikacji ze wszystkich procesów
    double sum_comp_time = 0.0;
    double sum_comm_time = 0.0;
    MPI_Reduce(&local_comp_time, &sum_comp_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_comm_time, &sum_comm_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    // 2. Proces główny buduje Drzewo Merkle'a, wykrywa duplikaty i wypisuje wyniki
    if (rank == 0) {
        double post_start = MPI_Wtime();

        std::vector<std::string> leaves(tasks.size());
        for (size_t i = 0; i < tasks.size(); ++i) {
            leaves[i] = tasks[i].sha256_hash;
        }
        std::string merkle_root = build_merkle_tree(std::move(leaves));

        std::map<std::string, std::vector<std::string>> dedup_map;
        for (const auto& t : tasks) {
            dedup_map[t.sha256_hash].push_back(t.rel_path);
        }

        size_t duplicate_groups = 0;
        size_t redundant_files = 0;
        for (const auto& [hash, paths] : dedup_map) {
            if (paths.size() > 1) {
                duplicate_groups++;
                redundant_files += (paths.size() - 1);
            }
        }

        double post_time = MPI_Wtime() - post_start;
        double avg_comp_time = (sum_comp_time / num_procs) + post_time;
        double avg_comm_time = (sum_comm_time / num_procs);

        if (!output_manifest.empty()) {
            std::ofstream out(output_manifest);
            for (const auto& t : tasks) {
                out << t.sha256_hash << "  " << t.rel_path << "\n";
            }
        }

        std::cout << "MODE: " << mode << "\n";
        std::cout << "PROCS: " << num_procs << "\n";
        std::cout << "FILES_PROCESSED: " << tasks.size() << "\n";
        std::cout << "MERKLE_ROOT: " << merkle_root << "\n";
        std::cout << "UNIQUE_HASHES: " << dedup_map.size() << "\n";
        std::cout << "DUPLICATE_GROUPS: " << duplicate_groups << "\n";
        std::cout << "REDUNDANT_FILES: " << redundant_files << "\n";
        std::cout << std::fixed << std::setprecision(6);
        std::cout << "COMP_TIME: " << avg_comp_time << "\n";
        std::cout << "COMM_TIME: " << avg_comm_time << "\n";
    }

    MPI_Finalize();
    return 0;
}

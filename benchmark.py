import argparse
import csv
import hashlib
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import time


def sha256_of_file(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def generate_dataset(dataset_dir, manifest_path, num_files, min_kb, max_kb, seed=42):
    """Generuje minimum 100 plikow o calkowicie losowych rozmiarach z przedzialu [min_kb, max_kb]."""
    if num_files < 100:
        print(f"Uwaga: Zwiekszono liczbe plikow z {num_files} do 100 (wymog minimum 100 plikow).")
        num_files = 100

    random.seed(seed)
    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)
    os.makedirs(dataset_dir, exist_ok=True)

    print(f"[1/4] Generowanie {num_files} losowych plikow ({min_kb} KB - {max_kb} KB) w katalogu '{dataset_dir}'...")

    num_subdirs = max(5, num_files // 20)
    base_chunk = random.randbytes(1024 * 1024)
    duplicate_payload = random.randbytes(max(1024, min_kb * 1024))

    total_bytes = 0
    for i in range(num_files):
        subdir = os.path.join(dataset_dir, f"batch_{i % num_subdirs}")
        os.makedirs(subdir, exist_ok=True)
        file_path = os.path.join(subdir, f"file_{i:04d}.bin")

        with open(file_path, "wb") as f:
            # Pierwsze 4 pliki maja identyczna zawartosc (test deduplikacji)
            if i < 4:
                f.write(duplicate_payload)
                total_bytes += len(duplicate_payload)
            else:
                # Calkowicie losowy rozmiar pliku (brak przewidywalnosci, ktory plik jest duzy)
                size_bytes = random.randint(min_kb * 1024, max_kb * 1024)

                header = f"FILE_ID_{i}_SEED_{seed}_SIZE_{size_bytes}_".encode("utf-8")
                f.write(header)
                remaining = size_bytes - len(header)

                while remaining > 0:
                    to_write = min(remaining, len(base_chunk))
                    f.write(base_chunk[:to_write])
                    remaining -= to_write

                total_bytes += size_bytes

    manifest_entries = []
    for root, _, files in os.walk(dataset_dir):
        for fname in files:
            full_p = os.path.join(root, fname)
            rel_p = os.path.relpath(full_p, dataset_dir).replace("\\", "/")
            manifest_entries.append((sha256_of_file(full_p), rel_p))

    manifest_entries.sort(key=lambda x: x[1])
    with open(manifest_path, "w", encoding="utf-8") as mf:
        for h, rel_p in manifest_entries:
            mf.write(f"{h}  {rel_p}\n")

    total_mb = total_bytes / (1024 * 1024)
    print(f"      -> Wygenerowano {len(manifest_entries)} plikow (lacznie {total_mb:.1f} MB) oraz '{manifest_path}'.")


def compile_cpp(cpp_source, binary_path):
    print(f"[2/4] Kompilacja programu C++ MPI ({cpp_source})...")
    cmd = ["mpicxx", "-O3", "-std=c++17", cpp_source, "-o", binary_path]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("BLAD KOMPILACJI:\n", res.stderr)
        sys.exit(1)
    print("      -> Kompilacja zakonczona sukcesem.")


def run_and_time_cpp(binary_path, dataset_dir, procs, mode, output_manifest=None):
    """Uruchamia program C++ przez mpirun i mierzy czas jego dzialania po stronie Pythona."""
    cmd = [
        "mpirun",
        "--oversubscribe",
        "-np", str(procs),
        binary_path,
        "-d", dataset_dir,
        "-m", mode,
    ]
    if output_manifest:
        cmd.extend(["-o", output_manifest])

    start_time = time.perf_counter()
    res = subprocess.run(cmd, capture_output=True, text=True)
    elapsed_time = time.perf_counter() - start_time

    if res.returncode != 0:
        raise RuntimeError(f"Blad wykonania MPI:\n{res.stderr}")

    parsed = {"TIME_SECONDS": elapsed_time}
    for key in ["MERKLE_ROOT", "FILES_PROCESSED", "UNIQUE_HASHES", "DUPLICATE_GROUPS", "REDUNDANT_FILES", "COMP_TIME", "COMM_TIME"]:
        match = re.search(rf"{key}:\s*(\S+)", res.stdout)
        if match:
            parsed[key] = match.group(1)

    return parsed


def verify_correctness(binary_path, dataset_dir, reference_manifest, modes, proc_list):
    print("[3/4] Weryfikacja poprawnosci obliczen C++ MPI wzgledem Pythona...")
    temp_manifest = "cpp_output.sha256"

    base_res = run_and_time_cpp(binary_path, dataset_dir, procs=1, mode="blocking", output_manifest=temp_manifest)

    with open(reference_manifest, "r", encoding="utf-8") as f1, open(temp_manifest, "r", encoding="utf-8") as f2:
        ref_lines = [line.strip() for line in f1 if line.strip()]
        cpp_lines = [line.strip() for line in f2 if line.strip()]

    if ref_lines != cpp_lines:
        print("      [BLAD] Skroty wyliczone przez C++ roznia sie od wzorca!")
        sys.exit(1)

    print(f"      [OK] Wszystkie skroty SHA-256 ({len(cpp_lines)} plikow) sa zgodne.")

    for mode in modes:
        for p in [proc_list[0], proc_list[-1]]:
            r = run_and_time_cpp(binary_path, dataset_dir, procs=p, mode=mode)
            if r["MERKLE_ROOT"] != base_res["MERKLE_ROOT"]:
                print(f"      [BLAD] Rozny MERKLE_ROOT dla mode={mode}, procs={p}!")
                sys.exit(1)

    print(f"      [OK] Korzen Drzewa Merkle'a spojny: {base_res['MERKLE_ROOT'][:32]}...")
    print(f"      [OK] Deduplikacja: wykryto {base_res['DUPLICATE_GROUPS']} grupe ({base_res['REDUNDANT_FILES']} duplikaty).")

    if os.path.exists(temp_manifest):
        os.remove(temp_manifest)


def run_benchmarks(binary_path, dataset_dir, modes, proc_list, repeats, csv_output):
    print(f"[4/4] Pomiary wydajnosci w Pythonie ({repeats} powtorzen dla kazdej konfiguracji)...")
    print("-" * 115)
    print(f"{'Tryb (Flaga)':<14} | {'Procesy':<7} | {'Mediana [s]':<12} | {'Srednia [s]':<12} | {'Oblicz. [s]':<11} | {'Komunik. [s]':<12} | {'Przysp. S(p)':<12} | {'Efekt. E(p)':<10}")
    print("-" * 115)

    rows = []
    for mode in modes:
        t1_median = None
        for p in proc_list:
            times = []
            comp_times = []
            comm_times = []
            for _ in range(repeats):
                metrics = run_and_time_cpp(binary_path, dataset_dir, procs=p, mode=mode)
                times.append(metrics["TIME_SECONDS"])
                comp_times.append(float(metrics.get("COMP_TIME", 0.0)))
                comm_times.append(float(metrics.get("COMM_TIME", 0.0)))

            med = statistics.median(times)
            avg = statistics.mean(times)
            std = statistics.stdev(times) if len(times) > 1 else 0.0
            med_comp = statistics.median(comp_times)
            med_comm = statistics.median(comm_times)

            if p == proc_list[0]:
                t1_median = med

            speedup = t1_median / med if med > 0 else 0.0
            efficiency = speedup / p

            print(f"{mode:<14} | {p:<7} | {med:<12.6f} | {avg:<12.6f} | {med_comp:<11.6f} | {med_comm:<12.6f} | {speedup:<12.2f} | {efficiency:<10.2%}")
            rows.append({
                "mode": mode,
                "processes": p,
                "median_s": round(med, 6),
                "mean_s": round(avg, 6),
                "stdev_s": round(std, 6),
                "comp_s": round(med_comp, 6),
                "comm_s": round(med_comm, 6),
                "speedup": round(speedup, 4),
                "efficiency": round(efficiency, 4),
            })
        print("-" * 115)

    with open(csv_output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["mode", "processes", "median_s", "mean_s", "stdev_s", "comp_s", "comm_s", "speedup", "efficiency"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Zapisano wyniki benchmarkow do pliku: {csv_output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generator losowych danych i benchmark programu C++ MPI SHA-256 / Merkle Tree")
    parser.add_argument("-d", "--dir", default="generated_data", help="Katalog na wygenerowane pliki")
    parser.add_argument("-n", "--files", type=int, default=120, help="Liczba plikow do wygenerowania (minimum 100, domyslnie: 120)")
    parser.add_argument("--min-kb", type=int, default=512, help="Minimalny rozmiar pliku w KB (domyslnie: 512 KB)")
    parser.add_argument("--max-kb", type=int, default=16384, help="Maksymalny rozmiar pliku w KB (domyslnie: 16384 KB = 16 MB)")
    parser.add_argument("-r", "--repeats", type=int, default=5, help="Liczba powtorzen pomiaru czasu (domyslnie: 5)")
    parser.add_argument("-t", "--threads", nargs="+", type=int, default=[1, 2, 4, 8, 16], help="Lista liczby procesow MPI do przetestowania")
    args = parser.parse_args()

    proc_list = sorted(list(set(args.threads)))
    modes = ["blocking", "nonblocking"]
    binary_path = "./sha256_worker"
    manifest_path = "reference_manifest.sha256"

    generate_dataset(args.dir, manifest_path, args.files, args.min_kb, args.max_kb)
    compile_cpp("main.cpp", binary_path)
    verify_correctness(binary_path, args.dir, manifest_path, modes, proc_list)
    run_benchmarks(binary_path, args.dir, modes, proc_list, args.repeats, "wyniki_pomiarow.csv")

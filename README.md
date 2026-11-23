# Projekt 2 (MPI): Liczenie skrótów SHA-256, Drzewo Merkle'a i Deduplikacja

Projekt z przedmiotu **Programowanie Równoległe**. Program dzieli losowej wielkości pliki pomiędzy procesy MPI, równolegle wylicza skróty SHA-256, przesyła je do procesu głównego (w trybie blokującym `blocking` lub nieblokującym `nonblocking`), buduje Drzewo Merkle'a i wyszukuje duplikaty plików.

---

## Za co odpowiadają poszczególne pliki

- **`sha256.hpp`** — odpowiada za wyliczanie skrótu SHA-256 dla pojedynczego pliku.
- **`main.cpp`** — główny program w C++ (MPI). Pobiera wszystkie pliki z katalogu, rozdziela pracę na procesy, przesyła obliczone skróty do procesu głównego, buduje Drzewo Merkle'a i wypisuje liczbę duplikatów.
- **`benchmark.py`** — program testujący w Pythonie. Generuje pliki o całkowicie losowych rozmiarach, kompiluje kod C++ (`mpicxx`), sprawdza poprawność wyników i mierzy czas działania dla 1, 2, 4, 8 i 16 procesów.
- **`wyniki_pomiarow.csv`** — tabela z zapisanymi czasami, przyspieszeniem i efektywnością.

---

## Jak używać programu

### Sposób 1: Uruchomienie wszystkiego automatycznie przez Pythona (Zalecane)

```bash
# Jednorazowa instalacja OpenMPI na Ubuntu (jeśli jeszcze nie ma):
sudo apt update && sudo apt install -y g++ python3 openmpi-bin libopenmpi-dev

# Domyślne uruchomienie (tworzy 120 losowych plików i testuje dla 1, 2, 4, 8 i 16 procesów):
python3 benchmark.py

# Test na 400 losowych plikach dla 1, 2, 4, 8 i 16 procesów (po 5 powtórzeń):
python3 benchmark.py --files 400 --min-kb 2048 --max-kb 16384 --threads 1 2 4 8 16 --repeats 5
# Testy przeprowadzone 

python3 benchmark.py --files 100 --min-kb 512 --max-kb 4096 --threads 1 --repeats 5
python3 benchmark.py --files 200 --min-kb 512 --max-kb 4096 --threads 2 --repeats 5
python3 benchmark.py --files 400 --min-kb 512 --max-kb 4096 --threads 4 --repeats 5
python3 benchmark.py --files 800 --min-kb 512 --max-kb 4096 --threads 8 --repeats 5

```

### Sposób 2: Ręczna kompilacja i uruchomienie programu C++

```bash 
# Kompilacja:
mpicxx -O3 -std=c++17 main.cpp -o sha256_worker

# Uruchomienie na 4 procesach w trybie blokującym:
mpirun -np 4 ./sha256_worker -d generated_data -m blocking

# Uruchomienie na 8 procesach w trybie nieblokującym:
mpirun -np 8 ./sha256_worker -d generated_data -m nonblocking
```

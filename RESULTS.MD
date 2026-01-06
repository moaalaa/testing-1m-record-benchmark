# Database Ingestion Benchmark Results

This file contains the results of all CSV → Database ingestion benchmarks, including time, throughput, memory, and CPU metrics.

---

## 🗂️ Summary Table

| DB    | Mode     | Variant | Language | Total Rows | Total Time (s) | Rows/sec | Peak Memory (MB) | Peak CPU (%) |
| ----- | -------- | ------- | -------- | ---------- | -------------- | -------- | ---------------- | ------------ |
| MySQL | Boring   | Plain   | PHP      | 1000000    | 94.85          | 10542.89 | 8                |              |
| MySQL | Boring   | Index   | PHP      |            |                |          |                  |              |
| MySQL | Boring   | Load    | PHP      |            |                |          |                  |              |
| MySQL | Boring   | Memory  | PHP      |            |                |          |                  |              |
| MySQL | Parallel | Plain   | PHP      |            |                |          |                  |              |
| ...   | ...      | ...     | ...      | ...        | ...            | ...      | ...              | ...          |

> Copy this table for each language (Node.js, Python, Go, Rust, Dart)

---

## 📊 Graphs

### 1. Total Time per Scenario

![Total Time Graph](graphs/combined_total_time.png)

### 2. Rows/sec per Scenario

![Rows/sec Graph](graphs/combined_rows_per_sec.png)

### 3. Memory Usage over Time

![Memory Usage Graph](graphs/MySQL_Boring_Plain_PHP_memory.png)

### 4. Memory Spikes over Time

![Memory Spikes Graph](graphs/MySQL_Boring_Plain_PHP_memory.png)

### 5. CPU Usage over Time

![CPU Usage Graph](graphs/MySQL_Boring_Plain_PHP_cpu.png)

### 6. CPU Spikes over Time

![CPU Spikes Graph](graphs/MySQL_Boring_Plain_PHP_cpu.png)

---

## ⚡ Notes

- Each run is repeated 3 times; reported metrics are **averages**
- Errors and exceptions are logged per run (see `logs/` folder if applicable)
- Graphs are generated from JSON metrics files located in `results/`

---

## 🗂️ Per-Language Sections

You can optionally add a section per language with **all variants detailed**:

### PHP

| DB         | Mode     | Variant | Total Time (s) | Rows/sec | Peak Memory (MB) | Peak CPU (%) |
| ---------- | -------- | ------- | -------------- | -------- | ---------------- | ------------ |
| MySQL      | Boring   | Plain   | 94.85          | 10542.89 | 8                |              |
| MySQL      | Parallel | Plain   |                |          |                  |              |
| PostgreSQL | Boring   | Copy    |                |          |                  |              |
| ...        | ...      | ...     | ...            | ...      | ...              | ...          |

#### Graphs

- Total Time: ![Graph](graphs/php_total_time.png)
- Rows/sec: ![Graph](graphs/php_rows_per_sec.png)
- Memory Usage: ![Graph](graphs/php_memory.png)
- Memory Spikes: ![Graph](graphs/php_memory_spikes.png)
- CPU Usage: ![Graph](graphs/php_cpu.png)
- CPU Spikes: ![Graph](graphs/php_cpu_spikes.png)
- Total Time: ![Graph](graphs/MySQL_Boring_Plain_PHP_total_time.png)
- Rows/sec: ![Graph](graphs/MySQL_Boring_Plain_PHP_rows_per_sec.png)
- Memory Usage: ![Graph](graphs/MySQL_Boring_Plain_PHP_memory.png)
- Memory Spikes: ![Graph](graphs/MySQL_Boring_Plain_PHP_memory.png)
- CPU Usage: ![Graph](graphs/MySQL_Boring_Plain_PHP_cpu.png)
- CPU Spikes: ![Graph](graphs/MySQL_Boring_Plain_PHP_cpu.png)

> Repeat the above for Node.js, Python, Go, Rust, Dart

---

## ✅ Usage

1. Run benchmarks per scenario → generates JSON in `results/`
2. Copy metrics into the tables above
3. Generate graphs using Python script → save into `graphs/`
4. Replace placeholders in this file with graph images
5. Review results and compare across languages, DBs, modes, and variants

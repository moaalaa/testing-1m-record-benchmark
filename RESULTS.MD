# Database Ingestion Benchmark Results

This file contains the results of all CSV → Database ingestion benchmarks, including time, throughput, memory, and CPU metrics.

---

## 🗂️ Summary Table

| DB    | Mode     | Variant | Language | Total Rows | Total Time (s) | Rows/sec | Peak Memory (MB) | Peak CPU (%) |
| ----- | -------- | ------- | -------- | ---------- | -------------- | -------- | ---------------- | ------------ |
| MySQL | Boring   | Plain   | PHP      |            |                |          |                  |              |
| MySQL | Boring   | Index   | PHP      |            |                |          |                  |              |
| MySQL | Boring   | Load    | PHP      |            |                |          |                  |              |
| MySQL | Boring   | Memory  | PHP      |            |                |          |                  |              |
| MySQL | Parallel | Plain   | PHP      |            |                |          |                  |              |
| ...   | ...      | ...     | ...      | ...        | ...            | ...      | ...              | ...          |

> Copy this table for each language (Node.js, Python, Go, Rust, Dart)

---

## 📊 Graphs

### 1. Total Time per Scenario

![Total Time Graph](graphs/total_time_PLACEHOLDER.png)

### 2. Rows/sec per Scenario

![Rows/sec Graph](graphs/rows_per_sec_PLACEHOLDER.png)

### 3. Memory Usage over Time

![Memory Usage Graph](graphs/memory_USAGE_PLACEHOLDER.png)

### 4. Memory Spikes over Time

![Memory Spikes Graph](graphs/memory_SPIKES_PLACEHOLDER.png)

### 5. CPU Usage over Time

![CPU Usage Graph](graphs/cpu_USAGE_PLACEHOLDER.png)

### 6. CPU Spikes over Time

![CPU Spikes Graph](graphs/cpu_SPIKES_PLACEHOLDER.png)

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
| MySQL      | Boring   | Plain   |                |          |                  |              |
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

> Repeat the above for Node.js, Python, Go, Rust, Dart

---

## ✅ Usage

1. Run benchmarks per scenario → generates JSON in `results/`
2. Copy metrics into the tables above
3. Generate graphs using Python script → save into `graphs/`
4. Replace placeholders in this file with graph images
5. Review results and compare across languages, DBs, modes, and variants

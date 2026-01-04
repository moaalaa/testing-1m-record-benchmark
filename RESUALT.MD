# Benchmark Results Template

Use this file to record results for all test runs. Fill in the tables and graphs after each benchmark execution.

---

## 📅 Test Info

- Date: YYYY-MM-DD
- Hardware: CPU / RAM / OS
- MySQL Version:
- PostgreSQL Version:
- CSV file: 1,000,000 rows, shared across all tests

---

## 1️⃣ Test Results Table

| DB         | Mode     | Variant      | Language | Total Time (s) | Rows/sec | Peak Memory (MB) | CPU Max (%) | Notes |
| ---------- | -------- | ------------ | -------- | -------------- | -------- | ---------------- | ----------- | ----- |
| MySQL      | Boring   | Plain        | PHP      |                |          |                  |             |       |
| MySQL      | Boring   | Index ON     | PHP      |                |          |                  |             |       |
| MySQL      | Boring   | LOAD DATA    | PHP      |                |          |                  |             |       |
| MySQL      | Boring   | Memory Limit | PHP      |                |          |                  |             |       |
| MySQL      | Parallel | Plain        | PHP      |                |          |                  |             |       |
| MySQL      | Parallel | Index ON     | PHP      |                |          |                  |             |       |
| MySQL      | Parallel | LOAD DATA    | PHP      |                |          |                  |             |       |
| MySQL      | Parallel | Memory Limit | PHP      |                |          |                  |             |       |
| PostgreSQL | Boring   | Plain        | PHP      |                |          |                  |             |       |
| PostgreSQL | Boring   | Index ON     | PHP      |                |          |                  |             |       |
| PostgreSQL | Boring   | COPY         | PHP      |                |          |                  |             |       |
| PostgreSQL | Boring   | Memory Limit | PHP      |                |          |                  |             |       |
| PostgreSQL | Parallel | Plain        | PHP      |                |          |                  |             |       |
| PostgreSQL | Parallel | Index ON     | PHP      |                |          |                  |             |       |
| PostgreSQL | Parallel | COPY         | PHP      |                |          |                  |             |       |
| PostgreSQL | Parallel | Memory Limit | PHP      |                |          |                  |             |       |

> Repeat the table for each language (Node, Python, Go, Rust, Dart)

---

## 2️⃣ Graph Placeholders

### Total Time per Language

```
Language | MySQL Boring | MySQL Parallel | Postgres Boring | Postgres Parallel
PHP      |
Node     |
Python   |
Go       |
Rust     |
Dart     |
```

### Speedup: Boring vs Parallel

```
Language | MySQL Speedup | Postgres Speedup
PHP      |
Node     |
Python   |
Go       |
Rust     |
Dart     |
```

### Index Degradation

```
Language | MySQL Plain | MySQL Index | Postgres Plain | Postgres Index
PHP      |
Node     |
Python   |
Go       |
Rust     |
Dart     |
```

### DB Comparison

```
Language | Boring MySQL | Boring Postgres | Parallel MySQL | Parallel Postgres
PHP      |
Node     |
Python   |
Go       |
Rust     |
Dart     |
```

### Memory Usage over Time (line graph)

- X-axis: Time (s)
- Y-axis: Memory (MB)
- One line per worker or language

### CPU Usage over Time (line graph)

- X-axis: Time (s)
- Y-axis: CPU (%)
- One line per worker or language

---

## 3️⃣ Notes

- Record any anomalies, errors, or spikes
- Record OS-level events that may have affected benchmark
- Indicate warm/cold DB cache if relevant
- Include screenshots or exported graphs if generated

---

This template is designed to \*\*capture all necessary metrics and visualize performance trend

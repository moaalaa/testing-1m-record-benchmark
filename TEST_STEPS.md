1. Collect metrics into results/

2. After all runs, fill RESULTS.md and generate graphs:

- Total time
- Rows/sec
- CPU usage over time
- Memory usage over time

| Language | CSV Parsing               | Database                   | Metrics / CPU & Memory                   | Progress                     |
| -------- | ------------------------- | -------------------------- | ---------------------------------------- | ---------------------------- |
| PHP      | `league/csv` (optional)   | PDO (built-in)             | `memory_get_usage()`, `sys_getloadavg()` | `symfony/console` (optional) |
| Node.js  | `csv-parser`              | `mysql2` / `pg`            | `process.memoryUsage()`, `perf_hooks`    | `progress`                   |
| Python   | `csv` (built-in)          | `PyMySQL` / `psycopg2`     | `psutil`                                 | `tqdm`                       |
| Go       | `encoding/csv` (built-in) | `database/sql` + driver    | `runtime`                                | custom log / fmt             |
| Rust     | `csv` crate               | `mysql` / `postgres` crate | `sysinfo` crate                          | custom log                   |
| Dart     | `csv` package             | `mysql1` / `postgres`      | `ProcessInfo`                            | console / log                |

## Mysql Tables

```sql
-- MySQL: Create tables for all modes and variants

-- Boring Mode
CREATE TABLE IF NOT EXISTS products_mysql_boring_plain (
    id INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Description TEXT,
    Brand VARCHAR(255),
    Category VARCHAR(100),
    Price DECIMAL(10,2),
    Currency CHAR(3),
    Stock INT,
    EAN VARCHAR(20),
    Color VARCHAR(50),
    Size VARCHAR(50),
    Availability ENUM('in_stock','out_of_stock', 'limited_stock', 'discontinued', 'pre_order', 'backorder'),
    InternalID INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS products_mysql_boring_index LIKE products_mysql_boring_plain;
CREATE TABLE IF NOT EXISTS products_mysql_boring_load LIKE products_mysql_boring_plain;
CREATE TABLE IF NOT EXISTS products_mysql_boring_memory LIKE products_mysql_boring_plain;

-- Parallel Mode
CREATE TABLE IF NOT EXISTS products_mysql_parallel_plain LIKE products_mysql_boring_plain;
CREATE TABLE IF NOT EXISTS products_mysql_parallel_index LIKE products_mysql_boring_plain;
CREATE TABLE IF NOT EXISTS products_mysql_parallel_load LIKE products_mysql_boring_plain;
CREATE TABLE IF NOT EXISTS products_mysql_parallel_memory LIKE products_mysql_boring_plain;
```

### Mysql Indexing test

```sql
ALTER TABLE products_mysql_boring_index ADD INDEX idx_brand_category (Brand, Category);
ALTER TABLE products_mysql_parallel_index ADD INDEX idx_brand_category (Brand, Category);
```

## Postgress Tables

```sql
-- PostgreSQL: Create tables for all modes and variants

-- Boring Mode
CREATE TABLE IF NOT EXISTS products_pg_boring_plain (
    id INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Description TEXT,
    Brand VARCHAR(255),
    Category VARCHAR(100),
    Price NUMERIC(10,2),
    Currency CHAR(3),
    Stock INT,
    EAN VARCHAR(20),
    Color VARCHAR(50),
    Size VARCHAR(50),
    Availability VARCHAR(20) CHECK (Availability IN ('in_stock','out_of_stock', 'limited_stock', 'discontinued', 'pre_order', 'backorder')),
    InternalID INT
);

CREATE TABLE IF NOT EXISTS products_pg_boring_index (LIKE products_pg_boring_plain INCLUDING ALL);
CREATE TABLE IF NOT EXISTS products_pg_boring_copy (LIKE products_pg_boring_plain INCLUDING ALL);
CREATE TABLE IF NOT EXISTS products_pg_boring_memory (LIKE products_pg_boring_plain INCLUDING ALL);

-- Parallel Mode
CREATE TABLE IF NOT EXISTS products_pg_parallel_plain (LIKE products_pg_boring_plain INCLUDING ALL);
CREATE TABLE IF NOT EXISTS products_pg_parallel_index (LIKE products_pg_boring_plain INCLUDING ALL);
CREATE TABLE IF NOT EXISTS products_pg_parallel_copy (LIKE products_pg_boring_plain INCLUDING ALL);
CREATE TABLE IF NOT EXISTS products_pg_parallel_memory (LIKE products_pg_boring_plain INCLUDING ALL);

```

### Postgress Indexing test

```sql
CREATE INDEX idx_brand_category_boring ON products_pg_boring_index (Brand, Category);
CREATE INDEX idx_brand_category_parallel ON products_pg_parallel_index (Brand, Category);
```

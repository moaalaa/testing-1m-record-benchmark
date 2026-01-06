import csv
import time
import json
import psutil
import os
import MySQLdb

# ---------------- CONFIG ----------------
import sys

# Resolve paths relative to this script file so running from different CWDs works
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.normpath(os.path.join(BASE_DIR, '..', '..', '..', 'test-file.csv'))
RESULTS_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', '..', '..', 'results'))
TABLE = "products_mysql_boring_plain"

BATCH_SIZE = 1000
TOTAL_ROWS = 1_000_000

# ---------------- METRICS ----------------
memory_usage = []
memory_spikes = []
cpu_usage = []
cpu_spikes = []

rows_inserted = 0
process = psutil.Process(os.getpid())


# ---------------- HELPERS ----------------
def record_metrics():
    mem = process.memory_info().rss / (1024 * 1024)  # MB
    cpu = process.cpu_percent(interval=None)

    memory_usage.append(mem)
    memory_spikes.append(max(mem, memory_spikes[-1] if memory_spikes else 0))

    cpu_usage.append(cpu)
    cpu_spikes.append(max(cpu, cpu_spikes[-1] if cpu_spikes else 0))


def insert_batch(cursor, batch):
    sql = f"""
        INSERT INTO {TABLE}
        (id, Name, Description, Brand, Category, Price, Currency, Stock,
         EAN, Color, Size, Availability, `InternalID`)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cursor.executemany(sql, batch)


# ---------------- MAIN ----------------
def main():
    global rows_inserted

    os.makedirs(RESULTS_DIR, exist_ok=True)

    if not os.path.exists(CSV_FILE):
        print(f"ERROR: CSV file not found at expected path: {CSV_FILE}")
        print("Make sure you run the script from the project root or that the test-file.csv exists.")
        sys.exit(1)

    db = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="",
        db="benchmark",
        autocommit=False
    )

    cursor = db.cursor()

    cursor.execute(f"TRUNCATE TABLE {TABLE}")
    db.commit()

    batch = []

    start_time = time.time()
    last_log = 0
    
    with open(CSV_FILE, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            batch.append((
                row["Id"],
                row["Name"],
                row["Description"],
                row["Brand"],
                row["Category"],
                row["Price"],
                row["Currency"],
                row["Stock"],
                row["EAN"],
                row["Color"],
                row["Size"],
                row["Availability"],
                row["InternalID"],
            ))

            if len(batch) >= BATCH_SIZE:
                insert_batch(cursor, batch)
                db.commit()

                rows_inserted += len(batch)
                batch.clear()          # IMPORTANT: free references
                record_metrics()

                if rows_inserted - last_log >= 100_000:
                    print(f"Inserted {rows_inserted} rows")
                    last_log = rows_inserted

        if batch:
            insert_batch(cursor, batch)
            db.commit()
            rows_inserted += len(batch)
            batch.clear()
            record_metrics()

    total_time = time.time() - start_time
    rows_per_sec = rows_inserted / total_time

    result = {
        "db": "MySQL",
        "mode": "Boring",
        "variant": "Plain",
        "language": "Python",
        "total_rows": rows_inserted,
        "total_time_sec": round(total_time, 2),
        "rows_per_sec": round(rows_per_sec, 2),
        "peak_memory_mb": round(max(memory_usage), 2),
        "peak_cpu_percent": round(max(cpu_usage), 2),
        "memory_usage": memory_usage,
        "memory_spikes": memory_spikes,
        "cpu_usage": cpu_usage,
        "cpu_spikes": cpu_spikes,
    }

    with open(f"{RESULTS_DIR}/mysql_boring_a_plain_python.json", "w") as f:
        json.dump(result, f, indent=2)

    print("\n✅ DONE")
    print(f"Time: {total_time:.2f}s")
    print(f"Rows/sec: {rows_per_sec:.0f}")

    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
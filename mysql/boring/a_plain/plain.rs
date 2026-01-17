use csv::ReaderBuilder;
use mysql::*;
use mysql::prelude::*;
use serde::Serialize;
use std::fs::{File, create_dir_all};
use std::time::Instant;
use sysinfo::{System, SystemExt, ProcessExt};

const CSV_FILE: &str = "../../../test-file.csv";
const RESULTS_DIR: &str = "../../../results";
const TABLE: &str = "products_mysql_boring_plain";
const BATCH_SIZE: usize = 1000;

#[derive(Serialize)]
struct ResultMetrics {
    db: String,
    mode: String,
    variant: String,
    language: String,
    total_rows: usize,
    total_time_sec: f64,
    rows_per_sec: f64,
    peak_memory_mb: f64,
    peak_cpu_percent: f32,
    memory_usage: Vec<f64>,
    memory_spikes: Vec<f64>,
    cpu_usage: Vec<f32>,
    cpu_spikes: Vec<f32>,
}

fn main() -> Result<()> {
    create_dir_all(RESULTS_DIR).unwrap();

    let url = "mysql://root:@localhost:3306/benchmark";
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;

    // Truncate table
    conn.query_drop(format!("TRUNCATE TABLE {}", TABLE))?;

    // CSV Reader
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(CSV_FILE)
        .unwrap();

    let mut batch: Vec<Vec<String>> = Vec::with_capacity(BATCH_SIZE);

    let mut system = System::new_all();
    let pid = sysinfo::get_current_pid().unwrap();
    let mut memory_usage = vec![];
    let mut memory_spikes = vec![];
    let mut cpu_usage = vec![];
    let mut cpu_spikes = vec![];

    let mut rows_inserted = 0;
    let mut last_log = 0;

    let start = Instant::now();

    for result in rdr.records() {
        let record = result.unwrap();
        let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        batch.push(row);

        if batch.len() >= BATCH_SIZE {
            insert_batch(&mut conn, &batch)?;
            rows_inserted += batch.len();
            batch.clear();

            // record metrics
            system.refresh_process(pid);
            let proc = system.process(pid).unwrap();
            let mem_mb = proc.memory() as f64 / 1024.0;
            let cpu_percent = proc.cpu_usage();

            memory_usage.push(mem_mb);
            memory_spikes.push(if memory_spikes.is_empty() { mem_mb } else { mem_mb.max(*memory_spikes.last().unwrap()) });
            cpu_usage.push(cpu_percent);
            cpu_spikes.push(if cpu_spikes.is_empty() { cpu_percent } else { cpu_percent.max(*cpu_spikes.last().unwrap()) });

            if rows_inserted - last_log >= 100_000 {
                println!("Inserted {} rows", rows_inserted);
                last_log = rows_inserted;
            }
        }
    }

    // Insert remaining batch
    if !batch.is_empty() {
        insert_batch(&mut conn, &batch)?;
        rows_inserted += batch.len();
        batch.clear();
    }

    let total_time = start.elapsed().as_secs_f64();
    let rows_per_sec = rows_inserted as f64 / total_time;

    let peak_mem = memory_usage.iter().cloned().fold(0./0., f64::max);
    let peak_cpu = cpu_usage.iter().cloned().fold(0./0., f32::max);

    let result = ResultMetrics {
        db: "MySQL".to_string(),
        mode: "Boring".to_string(),
        variant: "Plain".to_string(),
        language: "Rust".to_string(),
        total_rows: rows_inserted,
        total_time_sec: total_time,
        rows_per_sec,
        peak_memory_mb: peak_mem,
        peak_cpu_percent: peak_cpu,
        memory_usage,
        memory_spikes,
        cpu_usage,
        cpu_spikes,
    };

    let file_path = format!("{}/mysql_boring_a_plain_rust.json", RESULTS_DIR);
    let file = File::create(file_path).unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();

    println!("\n✅ DONE");
    println!("Time: {:.2}s", total_time);
    println!("Rows/sec: {:.0}", rows_per_sec);
    println!("Peak RAM: {:.2} MB", peak_mem);
    println!("Peak CPU: {:.2}%", peak_cpu);

    Ok(())
}

// Insert batch helper
fn insert_batch(conn: &mut PooledConn, batch: &[Vec<String>]) -> Result<()> {
    let mut params = vec![];
    for r in batch {
        let price: f64 = r[5].parse().unwrap_or(0.0);
        let stock: u32 = r[7].parse().unwrap_or(0);
        params.push((
            &r[0], &r[1], &r[2], &r[3], &r[4],
            price, &r[6], stock,
            &r[8], &r[9], &r[10], &r[11], &r[12]
        ));
    }

    let sql = format!("INSERT INTO {} (id, Name, Description, Brand, Category, Price, Currency, Stock, EAN, Color, Size, Availability, `Internal ID`) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)", TABLE);

    conn.exec_batch(sql, params)?;
    Ok(())
}

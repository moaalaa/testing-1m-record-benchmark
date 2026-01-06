package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shirou/gopsutil/v3/process"
)

const (
	csvFile    = "../../../test-file.csv"
	resultsDir = "../../../results"
	tableName  = "products_mysql_boring_plain"

	batchSize = 1000
	totalRows = 1000000
)

type Result struct {
	DB           string    `json:"db"`
	Mode         string    `json:"mode"`
	Variant      string    `json:"variant"`
	Language     string    `json:"language"`
	TotalRows    int       `json:"total_rows"`
	TotalTimeSec float64   `json:"total_time_sec"`
	RowsPerSec   float64   `json:"rows_per_sec"`
	PeakMemoryMB float64   `json:"peak_memory_mb"`
	PeakCPU      float64   `json:"peak_cpu_percent"`
	MemoryUsage  []float64 `json:"memory_usage"`
	MemorySpikes []float64 `json:"memory_spikes"`
	CPUUsage     []float64 `json:"cpu_usage"`
	CPUSpikes    []float64 `json:"cpu_spikes"`
}

func main() {
	_, thisFile, _, _ := runtime.Caller(0)

	baseDir := filepath.Dir(thisFile)

	resultsDir := filepath.Join(baseDir, resultsDir)

	os.MkdirAll(resultsDir, 0755)

	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/benchmark?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.Exec("TRUNCATE TABLE " + tableName)

	csvFile := filepath.Join(baseDir, csvFile)

	file, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	// skip header
	reader.Read()

	proc, _ := process.NewProcess(int32(os.Getpid()))

	start := time.Now()
	rowsInserted := 0
	lastLog := 0

	var memoryUsage, memorySpikes, cpuUsage, cpuSpikes []float64

	batch := make([][]string, 0, batchSize)

	recordMetrics := func() {
		memInfo, _ := proc.MemoryInfo()
		cpu, _ := proc.CPUPercent()

		mem := float64(memInfo.RSS) / 1024 / 1024

		memoryUsage = append(memoryUsage, mem)
		cpuUsage = append(cpuUsage, cpu)

		if len(memorySpikes) == 0 || mem > memorySpikes[len(memorySpikes)-1] {
			memorySpikes = append(memorySpikes, mem)
		} else {
			memorySpikes = append(memorySpikes, memorySpikes[len(memorySpikes)-1])
		}

		if len(cpuSpikes) == 0 || cpu > cpuSpikes[len(cpuSpikes)-1] {
			cpuSpikes = append(cpuSpikes, cpu)
		} else {
			cpuSpikes = append(cpuSpikes, cpuSpikes[len(cpuSpikes)-1])
		}
	}

	insertBatch := func(batch [][]string) {
		tx, _ := db.Begin()
		stmt, _ := tx.Prepare(`
			INSERT INTO ` + tableName + `
			(Id, Name, Description, Brand, Category, Price, Currency, Stock,
			 EAN, Color, Size, Availability, InternalID)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		defer stmt.Close()

		for _, r := range batch {
			price, _ := strconv.ParseFloat(r[5], 64)
			stock, _ := strconv.Atoi(r[7])

			stmt.Exec(
				r[0], r[1], r[2], r[3], r[4],
				price, r[6], stock,
				r[8], r[9], r[10], r[11], r[12],
			)
		}

		tx.Commit()
	}

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		batch = append(batch, row)

		if len(batch) >= batchSize {
			insertBatch(batch)
			rowsInserted += len(batch)
			batch = batch[:0]

			recordMetrics()

			if rowsInserted-lastLog >= 100000 {
				fmt.Println("Inserted", rowsInserted)
				lastLog = rowsInserted
			}
		}
	}

	if len(batch) > 0 {
		insertBatch(batch)
		rowsInserted += len(batch)
		recordMetrics()
	}

	elapsed := time.Since(start).Seconds()
	rowsPerSec := float64(rowsInserted) / elapsed

	var peakMem, peakCPU float64
	for _, m := range memoryUsage {
		if m > peakMem {
			peakMem = m
		}
	}
	for _, c := range cpuUsage {
		if c > peakCPU {
			peakCPU = c
		}
	}

	result := Result{
		DB:           "MySQL",
		Mode:         "Boring",
		Variant:      "Plain",
		Language:     "Go",
		TotalRows:    rowsInserted,
		TotalTimeSec: elapsed,
		RowsPerSec:   rowsPerSec,
		PeakMemoryMB: peakMem,
		PeakCPU:      peakCPU,
		MemoryUsage:  memoryUsage,
		MemorySpikes: memorySpikes,
		CPUUsage:     cpuUsage,
		CPUSpikes:    cpuSpikes,
	}

	f, _ := os.Create(resultsDir + "/mysql_boring_a_plain_go.json")
	json.NewEncoder(f).Encode(result)
	f.Close()

	fmt.Println("\n✅ DONE")
	fmt.Printf("Time: %.2fs\n", elapsed)
	fmt.Printf("Rows/sec: %.0f\n", rowsPerSec)
	fmt.Printf("Peak RAM: %.2f MB\n", peakMem)
	fmt.Printf("Peak CPU: %.2f%%\n", peakCPU)

	runtime.GC()
}

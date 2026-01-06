<?php

require __DIR__ . '/../../../vendor/autoload.php';

use League\Csv\Reader;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\ConsoleOutput;

// ---------------- PHP SETTINGS ----------------
ini_set('memory_limit', '512M'); // Set memory limit
set_time_limit(0); // Unlimited execution time

// Config
$csvFile = __DIR__ . '/../../../test-file.csv';
$resultsDir = __DIR__ . '/../../../results';
$dbHost = '127.0.0.1';
$dbName = 'benchmark';
$dbUser = 'root';
$dbPass = ''; // adjust
$table = 'products_mysql_boring_plain';
$batchSize = 1000;

// DB connection
$pdo = new PDO(
    "mysql:host={$dbHost};dbname={$dbName};charset=utf8mb4",
    $dbUser,
    $dbPass,
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
);

// Truncate table before import
$pdo->exec("TRUNCATE TABLE `$table`");

// ---------------- CSV SETUP ----------------
$csv = Reader::from($csvFile, 'r');
$csv->setHeaderOffset(0);
$records = $csv->getRecords();


// ---------------- PROGRESS & METRICS ----------------
$output = new ConsoleOutput();
$progress = new ProgressBar($output);
$progress->start();

$startTime = microtime(true);
$rowsInserted = 0;
$memoryUsage = [];
$cpuUsage = [];
$batch = [];
$memorySpikes = [];
$cpuUsage = [];
$cpuSpikes = [];



// ---------------- BATCH INSERT ----------------
foreach ($records as $row) {
    $batch[] = $row;
    if (count($batch) >= $batchSize) {
        insertBatch($pdo, $table, array_keys($row), $batch);
        $rowsInserted += count($batch);
        $batch = [];
        $progress->setProgress($rowsInserted);
        recordMetrics($memoryUsage, $cpuUsage, $memorySpikes, $cpuSpikes);
    }
}

// Insert remaining rows
if (count($batch) > 0) {
    insertBatch($pdo, $table, array_keys($batch[0]), $batch);
    $rowsInserted += count($batch);
    $progress->setProgress($rowsInserted);
    recordMetrics($memoryUsage, $cpuUsage, $memorySpikes, $cpuSpikes);
}

$progress->finish();
$endTime = microtime(true);
$totalTime = $endTime - $startTime;
$rowsPerSec = $rowsInserted / $totalTime;


// ---------------- SAVE RESULTS ----------------
if (!is_dir($resultsDir)) mkdir($resultsDir, 0777, true);
file_put_contents($resultsDir . '/mysql_boring_a_plain_php.json', json_encode([
    'db' => 'MySQL',
    'mode' => 'Boring',
    'variant' => 'Plain',
    'language' => 'PHP',
    'total_rows' => $rowsInserted,
    'total_time_sec' => $totalTime,
    'rows_per_sec' => $rowsPerSec,
    'memory_usage' => $memoryUsage,
    'memory_spikes' => $memorySpikes,
    'cpu_usage' => $cpuUsage,
    'cpu_spikes' => $cpuSpikes,
], JSON_PRETTY_PRINT));

echo "\nBenchmark completed! Total time: {$totalTime}s, rows/sec: {$rowsPerSec}\n";

// ---------------- FUNCTIONS ----------------
function insertBatch(PDO $pdo, $table, $columns, $batch)
{
    $placeholders = '(' . implode(',', array_fill(0, count($columns), '?')) . ')';
    $sql = "INSERT INTO `$table` (`" . implode('`,`', $columns) . "`) VALUES ";
    $sql .= implode(',', array_fill(0, count($batch), $placeholders));
    $stmt = $pdo->prepare($sql);
    $flat = [];
    foreach ($batch as $row) {
        $flat = array_merge($flat, array_values($row));
    }
    $stmt->execute($flat);
}

// ---------------- METRICS ----------------
function recordMetrics(&$memoryUsage, &$cpuUsage, &$memorySpikes, &$cpuSpikes)
{
    // Memory
    $currentMem = memory_get_usage(true);
    $memoryUsage[] = $currentMem;
    $memorySpikes[] = max($currentMem, end($memorySpikes) ?? 0);

    // CPU
    if (function_exists('sys_getloadavg')) {
        $currentCpu = sys_getloadavg()[0] * 100; // approx %
    } else {
        $currentCpu = 0;
    }
    $cpuUsage[] = $currentCpu;
    $cpuSpikes[] = max($currentCpu, end($cpuSpikes) ?? 0); // peak CPU so far
}

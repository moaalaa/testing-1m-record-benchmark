import json
import matplotlib.pyplot as plt
import os
from pathlib import Path
from statistics import mean

# ---------------- CONFIG ----------------
results_dir = Path("./results")  # JSON results folder
output_dir = Path("./graphs")    # Save graphs here
output_dir.mkdir(parents=True, exist_ok=True)

# pick a preferred style that's actually available on the system
for _style in ('seaborn-darkgrid', 'seaborn', 'ggplot', 'bmh', 'classic'):
    if _style in plt.style.available:
        plt.style.use(_style)
        break
DEFAULT_FIGSIZE = (10, 6)
DEFAULT_DPI = 140

def bytes_to_mb(b):
    return b / 1024 / 1024

def annotate_bar(ax, rects, fmt="{:.2f}"):
    for rect in rects:
        h = rect.get_height()
        ax.annotate(fmt.format(h), xy=(rect.get_x() + rect.get_width() / 2, h),
                    xytext=(0, 6), textcoords="offset points", ha='center', va='bottom', fontsize=9)

def downsample(series, max_points=1200):
    n = len(series)
    if n <= max_points:
        return series
    step = max(1, n // max_points)
    return series[::step]

# ---------------- LOAD JSON FILES ----------------
json_files = [p for p in results_dir.iterdir() if p.suffix == ".json"]

# For combined summaries
summary_names = []
summary_total_time = []
summary_rows_per_sec = []
summary_peak_mem_mb = []

for path in sorted(json_files):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Skipping {path.name}: failed to load JSON ({e})")
        continue

    name = f"{data.get('db','?')}_{data.get('mode','?')}_{data.get('variant','?')}_{data.get('language','?')}"
    safe_name = name.replace(' ', '_')

    # collect summary values
    total_time = float(data.get('total_time_sec', 0) or 0)
    rows_per_sec = float(data.get('rows_per_sec', 0) or 0)

    # Determine peak memory (prefer memory_spikes if present)
    mem_spikes = data.get('memory_spikes') or []
    mem_usage = data.get('memory_usage') or []
    peak_bytes = 0
    if mem_spikes:
        peak_bytes = max(mem_spikes)
    elif mem_usage:
        peak_bytes = max(mem_usage)
    peak_mb = bytes_to_mb(peak_bytes) if peak_bytes else 0

    summary_names.append(name)
    summary_total_time.append(total_time)
    summary_rows_per_sec.append(rows_per_sec)
    summary_peak_mem_mb.append(peak_mb)

    # ---------------- TOTAL TIME ----------------
    fig, ax = plt.subplots(figsize=DEFAULT_FIGSIZE, dpi=DEFAULT_DPI)
    rects = ax.bar([name], [total_time], color="#4C72B0")
    ax.set_ylabel("Seconds")
    ax.set_title(f"Total Time — {name}")
    annotate_bar(ax, rects, fmt="{:.2f}")
    fig.tight_layout()
    fig.savefig(output_dir / f"{safe_name}_total_time.png")
    plt.close(fig)

    # ---------------- ROWS / SEC ----------------
    fig, ax = plt.subplots(figsize=DEFAULT_FIGSIZE, dpi=DEFAULT_DPI)
    rects = ax.bar([name], [rows_per_sec], color="#55A868")
    ax.set_ylabel("Rows / sec")
    ax.set_title(f"Rows per Second — {name}")
    annotate_bar(ax, rects, fmt="{:.2f}")
    fig.tight_layout()
    fig.savefig(output_dir / f"{safe_name}_rows_per_sec.png")
    plt.close(fig)

    # ---------------- MEMORY USAGE ----------------
    if mem_usage:
        mem_mb = [bytes_to_mb(v) for v in mem_usage]
        mem_mb_ds = downsample(mem_mb)
        spikes_mb = [bytes_to_mb(v) for v in mem_spikes] if mem_spikes else []
        spikes_mb_ds = downsample(spikes_mb) if spikes_mb else []

        fig, ax = plt.subplots(figsize=DEFAULT_FIGSIZE, dpi=DEFAULT_DPI)
        ax.plot(mem_mb_ds, label='Memory (MB)', color='#C44E52')
        if spikes_mb_ds:
            ax.plot(spikes_mb_ds, label='Memory Spikes (MB)', linestyle='--', color='#8172B2')
        ax.set_ylabel('MB')
        ax.set_xlabel('Sample')
        ax.set_title(f"Memory Usage — {name} (peak {peak_mb:.1f} MB)")
        ax.legend()
        fig.tight_layout()
        fig.savefig(output_dir / f"{safe_name}_memory.png")
        plt.close(fig)

    # ---------------- CPU USAGE ----------------
    cpu_usage = data.get('cpu_usage') or []
    cpu_spikes = data.get('cpu_spikes') or []
    if cpu_usage:
        cpu_ds = downsample(cpu_usage)
        spikes_ds = downsample(cpu_spikes) if cpu_spikes else []

        fig, ax = plt.subplots(figsize=DEFAULT_FIGSIZE, dpi=DEFAULT_DPI)
        ax.plot(cpu_ds, label='CPU %', color='#2E7D32')
        if spikes_ds:
            ax.plot(spikes_ds, label='CPU Spikes %', linestyle='--', color='#F28E2B')
        ax.set_ylabel('% CPU')
        ax.set_xlabel('Sample')
        ax.set_title(f"CPU Usage — {name}")
        ax.legend()
        fig.tight_layout()
        fig.savefig(output_dir / f"{safe_name}_cpu.png")
        plt.close(fig)
    else:
        # If no CPU data, write a small text file note
        note_path = output_dir / f"{safe_name}_cpu_missing.txt"
        with open(note_path, 'w', encoding='utf-8') as nf:
            nf.write('No CPU data found in JSON')

print("Per-file graphs generated in:", output_dir)

# ---------------- COMBINED SUMMARY CHARTS ----------------
if summary_names:
    # sort by total_time for a nicer layout
    idx = sorted(range(len(summary_total_time)), key=lambda i: summary_total_time[i], reverse=True)
    names_sorted = [summary_names[i] for i in idx]
    times_sorted = [summary_total_time[i] for i in idx]
    rows_sorted = [summary_rows_per_sec[i] for i in idx]
    mem_sorted = [summary_peak_mem_mb[i] for i in idx]

    # Total time (horizontal bars)
    fig, ax = plt.subplots(figsize=(12, max(6, len(names_sorted)*0.25)), dpi=DEFAULT_DPI)
    rects = ax.barh(names_sorted, times_sorted, color='#4C72B0')
    ax.set_xlabel('Seconds')
    ax.set_title('Total Time — All Scenarios (sorted)')
    annotate_bar(ax, rects, fmt="{:.2f}")
    fig.tight_layout()
    fig.savefig(output_dir / "combined_total_time.png")
    plt.close(fig)

    # Rows/sec (horizontal bars)
    fig, ax = plt.subplots(figsize=(12, max(6, len(names_sorted)*0.25)), dpi=DEFAULT_DPI)
    rects = ax.barh(names_sorted, rows_sorted, color='#55A868')
    ax.set_xlabel('Rows / sec')
    ax.set_title('Rows/sec — All Scenarios (sorted)')
    annotate_bar(ax, rects, fmt="{:.2f}")
    fig.tight_layout()
    fig.savefig(output_dir / "combined_rows_per_sec.png")
    plt.close(fig)

    # Peak memory (MB)
    fig, ax = plt.subplots(figsize=(12, max(6, len(names_sorted)*0.25)), dpi=DEFAULT_DPI)
    rects = ax.barh(names_sorted, mem_sorted, color='#C44E52')
    ax.set_xlabel('Peak Memory (MB)')
    ax.set_title('Peak Memory — All Scenarios (sorted)')
    annotate_bar(ax, rects, fmt="{:.1f}")
    fig.tight_layout()
    fig.savefig(output_dir / "combined_peak_memory_mb.png")
    plt.close(fig)

print("Summary charts saved in:", output_dir)

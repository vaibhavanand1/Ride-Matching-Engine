import os
import glob
import pandas as pd
import matplotlib.pyplot as plt


def generate_charts(results_dir: str = "results"):
    csv_files = glob.glob(os.path.join(results_dir, "*_stats.csv"))
    if not csv_files:
        print(f"No Locust stats CSV files found in {results_dir}")
        return

    records = []
    for f in csv_files:
        base = os.path.basename(f).replace("_stats.csv", "")
        # Format expected: {backend}_{driver_count} e.g. redis_100, h3_10000
        parts = base.split("_")
        if len(parts) != 2:
            continue
        backend, count_str = parts[0], parts[1]
        try:
            driver_count = int(count_str)
        except ValueError:
            continue

        try:
            df = pd.read_csv(f)
            req_row = df[df["Name"] == "/rides/request"]
            if req_row.empty:
                req_row = df[df["Name"] == "Aggregated"]
            if req_row.empty:
                continue

            row = req_row.iloc[0]
            records.append({
                "backend": backend.upper(),
                "driver_count": driver_count,
                "requests_sec": float(row.get("Requests/s", 0)),
                "p50": float(row.get("50%", row.get("Median Response Time", 0))),
                "p95": float(row.get("95%", 0)),
                "p99": float(row.get("99%", 0)),
            })
        except Exception as e:
            print(f"Error parsing {f}: {e}")

    if not records:
        print("No valid benchmark data could be parsed.")
        return

    data = pd.DataFrame(records).sort_values("driver_count")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    backends = data["backend"].unique()
    colors = {"REDIS": "#2563eb", "H3": "#dc2626"}
    markers = {"REDIS": "o", "H3": "s"}

    for b in backends:
        b_data = data[data["backend"] == b]
        c = colors.get(b, "#4b5563")
        m = markers.get(b, "o")

        ax1.plot(b_data["driver_count"], b_data["p50"], label=f"{b} p50", color=c, marker=m, linewidth=2)
        ax1.plot(b_data["driver_count"], b_data["p95"], label=f"{b} p95", color=c, marker=m, linestyle="--", alpha=0.7)
        ax1.plot(b_data["driver_count"], b_data["p99"], label=f"{b} p99", color=c, marker=m, linestyle=":", alpha=0.7)

    ax1.set_xscale("log")
    ax1.set_title("Matching Latency vs Driver Density")
    ax1.set_xlabel("Number of Simulated Drivers (Log Scale)")
    ax1.set_ylabel("Latency (ms)")
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend()

    for b in backends:
        b_data = data[data["backend"] == b]
        c = colors.get(b, "#4b5563")
        m = markers.get(b, "o")
        ax2.plot(b_data["driver_count"], b_data["requests_sec"], label=f"{b}", color=c, marker=m, linewidth=2)

    ax2.set_xscale("log")
    ax2.set_title("Throughput vs Driver Density")
    ax2.set_xlabel("Number of Simulated Drivers (Log Scale)")
    ax2.set_ylabel("Requests / Second")
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend()

    plt.tight_layout()
    chart_path = os.path.join(results_dir, "benchmark_comparison.png")
    plt.savefig(chart_path, dpi=200)
    plt.close()
    print(f"Benchmark comparison chart saved to: {chart_path}")


if __name__ == "__main__":
    generate_charts()

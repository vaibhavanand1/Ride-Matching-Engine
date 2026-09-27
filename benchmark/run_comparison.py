import argparse
import os
import random
import subprocess
import sys
import time
import pandas as pd

from common.redis_client import get_redis_client
from common.geo.redis_geo import RedisGeoBackend
from common.geo.h3_geo import H3GeoBackend
from common.database import SessionLocal, init_db
from common.models import Driver
from benchmark.plot_results import generate_charts

LAT_MIN, LAT_MAX = 37.685, 37.865
LON_MIN, LON_MAX = -122.529, -122.309


def populate_drivers(count: int, backend_name: str):
    print(f"Populating {count} drivers in DB and Redis for backend: {backend_name}...")
    init_db()
    db = SessionLocal()
    redis_conn = get_redis_client()

    redis_geo = RedisGeoBackend(redis_conn)
    h3_geo = H3GeoBackend(redis_conn)

    # Clean old geo keys
    redis_conn.delete("drivers:geo")
    # Flush h3 cells if needed
    for key in redis_conn.scan_iter("h3:*"):
        redis_conn.delete(key)
    for key in redis_conn.scan_iter("driver:cell:*"):
        redis_conn.delete(key)

    driver_ids = [f"drv_{i:05d}" for i in range(1, count + 1)]
    existing = set(
        d[0] for d in db.query(Driver.id).filter(Driver.id.in_(driver_ids)).all()
    )
    missing = [d_id for d_id in driver_ids if d_id not in existing]
    if missing:
        batch = [
            Driver(id=d_id, name=f"Driver-{d_id}", status="available")
            for d_id in missing
        ]
        db.bulk_save_objects(batch)
        db.commit()

    # Reset all drivers to available in Postgres
    db.query(Driver).filter(Driver.id.in_(driver_ids)).update(
        {Driver.status: "available"}, synchronize_session=False
    )
    db.commit()
    db.close()

    pipe = redis_conn.pipeline()
    for d_id in driver_ids:
        lat = random.uniform(LAT_MIN, LAT_MAX)
        lon = random.uniform(LON_MIN, LON_MAX)

        pipe.set(f"driver:status:{d_id}", "available")
        pipe.set(f"driver:last_seen:{d_id}", int(time.time()))

        if backend_name in ("redis", "both"):
            redis_geo.update_driver_location(d_id, lat, lon)
        if backend_name in ("h3", "both"):
            h3_geo.update_driver_location(d_id, lat, lon)

    pipe.execute()
    print(f"Successfully populated {count} drivers.")


def run_benchmark(
    backends,
    counts,
    users: int = 50,
    spawn_rate: int = 10,
    duration: str = "20s",
    host: str = "http://localhost:8000",
    results_dir: str = "results",
):
    os.makedirs(results_dir, exist_ok=True)

    for backend in backends:
        for count in counts:
            prefix = os.path.join(results_dir, f"{backend}_{count}")
            print(f"\n==========================================")
            print(f" Running Benchmark: {backend.upper()} with {count} drivers")
            print(f"==========================================")

            try:
                populate_drivers(count, backend)
            except Exception as e:
                print(f"Warning: Redis/DB population failed ({e}). Proceeding if data already seeded.")

            env = os.environ.copy()
            env["LOCUST_GEO_BACKEND"] = backend
            env["PYTHONPATH"] = "."

            cmd = [
                sys.executable,
                "-m",
                "locust",
                "-f",
                "locustfiles/ride_requests.py",
                "--headless",
                "-u",
                str(users),
                "-r",
                str(spawn_rate),
                "--run-time",
                duration,
                "--host",
                host,
                "--csv",
                prefix,
            ]
            print(f"Executing: {' '.join(cmd)}")
            subprocess.run(cmd, env=env, check=False)
            time.sleep(2)

    print("\nGenerating comparison charts...")
    generate_charts(results_dir)


def main():
    parser = argparse.ArgumentParser(description="Automated Geo Backend Benchmark Comparison")
    parser.add_argument("--backends", type=str, default="redis,h3", help="Comma-separated backends (redis,h3)")
    parser.add_argument("--counts", type=str, default="100,1000,10000", help="Comma-separated driver counts")
    parser.add_argument("-u", "--users", type=int, default=50, help="Number of concurrent Locust users")
    parser.add_argument("-r", "--spawn-rate", type=int, default=10, help="Locust spawn rate per second")
    parser.add_argument("-t", "--duration", type=str, default="20s", help="Locust run duration")
    parser.add_argument("--host", type=str, default="http://localhost:8000", help="Rider service target URL")
    parser.add_argument("--results-dir", type=str, default="results", help="Directory for output CSVs and charts")

    args = parser.parse_args()
    backends = [b.strip().lower() for b in args.backends.split(",")]
    counts = [int(c.strip()) for c in args.counts.split(",")]

    run_benchmark(
        backends=backends,
        counts=counts,
        users=args.users,
        spawn_rate=args.spawn_rate,
        duration=args.duration,
        host=args.host,
        results_dir=args.results_dir,
    )


if __name__ == "__main__":
    main()

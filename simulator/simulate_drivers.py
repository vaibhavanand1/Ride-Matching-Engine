import asyncio
import argparse
import random
import time
import sys
from typing import List, Tuple
import grpc

from proto import driver_pb2, driver_pb2_grpc
from common.database import SessionLocal, init_db
from common.models import Driver

# Roughly 20km x 20km centered on San Francisco
LAT_MIN, LAT_MAX = 37.685, 37.865
LON_MIN, LON_MAX = -122.529, -122.309


def ensure_drivers_in_db(driver_ids: List[str]):
    init_db()
    db = SessionLocal()
    try:
        existing = set(
            d[0] for d in db.query(Driver.id).filter(Driver.id.in_(driver_ids)).all()
        )
        missing = [d_id for d_id in driver_ids if d_id not in existing]
        if missing:
            print(f"Pre-populating {len(missing)} drivers in Postgres...")
            batch = [
                Driver(id=d_id, name=f"Driver-{d_id}", status="available")
                for d_id in missing
            ]
            db.bulk_save_objects(batch)
            db.commit()
    finally:
        db.close()


async def driver_stream_worker(
    stub: driver_pb2_grpc.DriverTrackingStub,
    driver_id: str,
    initial_lat: float,
    initial_lon: float,
    stop_event: asyncio.Event,
):
    lat = initial_lat
    lon = initial_lon

    async def request_generator():
        nonlocal lat, lon
        while not stop_event.is_set():
            # Random walk roughly 50m - 100m per step
            lat += random.uniform(-0.0008, 0.0008)
            lon += random.uniform(-0.0008, 0.0008)
            # Bounce off edges
            lat = max(LAT_MIN, min(LAT_MAX, lat))
            lon = max(LON_MIN, min(LON_MAX, lon))

            yield driver_pb2.LocationUpdate(
                driver_id=driver_id,
                latitude=lat,
                longitude=lon,
                timestamp=int(time.time()),
            )
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=random.uniform(2.0, 3.0)
                )
            except asyncio.TimeoutError:
                pass

    try:
        response_stream = stub.StreamLocation(request_generator())
        async for resp in response_stream:
            if stop_event.is_set():
                break
    except grpc.aio.AioRpcError as e:
        if not stop_event.is_set():
            print(f"Driver {driver_id} stream error: {e.code()}")
    except Exception:
        pass


async def run_simulation(
    num_drivers: int,
    target: str,
    duration: int,
    register_db: bool = True,
):
    driver_ids = [f"drv_{i:05d}" for i in range(1, num_drivers + 1)]

    if register_db:
        try:
            ensure_drivers_in_db(driver_ids)
        except Exception as e:
            print(f"DB registration skipped or failed ({e}); continuing with streaming.")

    print(f"Starting {num_drivers} simulated drivers streaming to {target}...")

    # Pool channels to stay well within OS socket limits for large driver counts
    num_channels = min(20, max(1, num_drivers // 100))
    channels = [grpc.aio.insecure_channel(target) for _ in range(num_channels)]
    stubs = [driver_pb2_grpc.DriverTrackingStub(ch) for ch in channels]

    stop_event = asyncio.Event()
    tasks = []

    for idx, driver_id in enumerate(driver_ids):
        stub = stubs[idx % num_channels]
        init_lat = random.uniform(LAT_MIN, LAT_MAX)
        init_lon = random.uniform(LON_MIN, LON_MAX)
        t = asyncio.create_task(
            driver_stream_worker(stub, driver_id, init_lat, init_lon, stop_event)
        )
        tasks.append(t)

    print(f"All {num_drivers} drivers connected and streaming.")

    try:
        if duration > 0:
            print(f"Simulating for {duration} seconds...")
            await asyncio.sleep(duration)
        else:
            print("Running continuously. Press Ctrl+C to stop.")
            while True:
                await asyncio.sleep(1)
    except (asyncio.CancelledError, KeyboardInterrupt):
        print("\nStopping simulator...")
    finally:
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        for ch in channels:
            await ch.close()
        print("Simulator stopped.")


def main():
    parser = argparse.ArgumentParser(description="Driver Location Simulator")
    parser.add_argument(
        "-n", "--drivers", type=int, default=100, help="Number of simulated drivers"
    )
    parser.add_argument(
        "--target",
        type=str,
        default="localhost:50051",
        help="gRPC server address (host:port)",
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        default=0,
        help="Run duration in seconds (0 = run indefinitely)",
    )
    parser.add_argument(
        "--no-db", action="store_true", help="Skip Postgres driver registration"
    )

    args = parser.parse_args()
    try:
        asyncio.run(
            run_simulation(
                num_drivers=args.drivers,
                target=args.target,
                duration=args.duration,
                register_db=not args.no_db,
            )
        )
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()

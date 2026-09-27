import json
from typing import Dict, Any, Optional
import redis
from sqlalchemy.orm import Session

from common.models import Driver, Ride, utcnow
from common.redis_client import DriverLock
from common.geo.base import GeoBackend


def match_ride(
    ride_id: str,
    pickup_lat: float,
    pickup_lon: float,
    db: Session,
    redis_conn: redis.Redis,
    geo: GeoBackend,
    radius_km: float = 5.0,
    max_candidates: int = 15,
) -> Dict[str, Any]:
    candidates = geo.find_nearby_drivers(
        lat=pickup_lat,
        lon=pickup_lon,
        radius_km=radius_km,
        limit=max_candidates,
    )

    matched_driver_id: Optional[str] = None

    for driver_id in candidates:
        lock = DriverLock(redis_conn, driver_id, ttl=5)
        if not lock.acquire():
            continue

        try:
            live_status = redis_conn.get(f"driver:status:{driver_id}")
            if live_status and live_status != "available":
                continue

            driver = db.query(Driver).filter(Driver.id == driver_id).first()
            if not driver or driver.status != "available":
                continue

            redis_conn.set(f"driver:status:{driver_id}", "busy")
            driver.status = "busy"

            ride = db.query(Ride).filter(Ride.id == ride_id).first()
            if ride:
                ride.driver_id = driver_id
                ride.status = "matched"
                ride.matched_at = utcnow()

            db.commit()
            matched_driver_id = driver_id
            break

        finally:
            lock.release()

    ride_record = db.query(Ride).filter(Ride.id == ride_id).first()
    channel = f"ride_updates:{ride_id}"

    if matched_driver_id:
        result = {
            "status": "matched",
            "ride_id": ride_id,
            "driver_id": matched_driver_id,
            "matched_at": (
                ride_record.matched_at.isoformat()
                if ride_record and ride_record.matched_at
                else None
            ),
        }
    else:
        if ride_record:
            ride_record.status = "no_driver_found"
            db.commit()
        result = {
            "status": "no_driver_found",
            "ride_id": ride_id,
            "driver_id": None,
            "matched_at": None,
        }

    redis_conn.publish(channel, json.dumps(result))
    return result

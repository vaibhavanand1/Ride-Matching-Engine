import concurrent.futures
import pytest
from common.database import SessionLocal
from common.models import Driver, Rider, Ride
from matching_service.matcher import match_ride


@pytest.mark.parametrize("backend_fixture", ["redis_geo", "h3_geo"])
def test_concurrent_match_single_driver(
    backend_fixture, request, db_session, redis_conn
):
    geo_backend = request.getfixturevalue(backend_fixture)

    solo_driver = Driver(id="solo_driver", name="Solo Driver", status="available")
    db_session.add(solo_driver)

    num_concurrent_riders = 15
    ride_ids = []

    for i in range(num_concurrent_riders):
        rider_id = f"rider_c_{i}"
        ride_id = f"ride_c_{i}"
        db_session.add(Rider(id=rider_id, name=f"Rider {i}"))
        db_session.add(
            Ride(
                id=ride_id,
                rider_id=rider_id,
                pickup_lat=37.7749,
                pickup_lon=-122.4194,
                status="pending",
            )
        )
        ride_ids.append(ride_id)

    db_session.commit()

    redis_conn.set("driver:status:solo_driver", "available")
    geo_backend.update_driver_location("solo_driver", 37.7749, -122.4194)

    def attempt_match(ride_id: str):
        thread_db = SessionLocal()
        try:
            return match_ride(
                ride_id=ride_id,
                pickup_lat=37.7749,
                pickup_lon=-122.4194,
                db=thread_db,
                redis_conn=redis_conn,
                geo=geo_backend,
            )
        finally:
            thread_db.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent_riders) as pool:
        futures = [pool.submit(attempt_match, r_id) for r_id in ride_ids]
        results = [f.result() for f in futures]

    matched_results = [r for r in results if r["status"] == "matched"]
    unmatched_results = [r for r in results if r["status"] == "no_driver_found"]

    assert len(matched_results) == 1, f"Expected 1 match, got {len(matched_results)}"
    assert matched_results[0]["driver_id"] == "solo_driver"
    assert len(unmatched_results) == num_concurrent_riders - 1

    matched_rides = (
        db_session.query(Ride).filter(Ride.driver_id == "solo_driver").all()
    )
    assert len(matched_rides) == 1
    assert matched_rides[0].id == matched_results[0]["ride_id"]
    assert matched_rides[0].status == "matched"

    db_session.refresh(solo_driver)
    assert solo_driver.status == "busy"
    assert redis_conn.get("driver:status:solo_driver") == "busy"

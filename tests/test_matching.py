import json
from common.models import Driver, Rider, Ride
from matching_service.matcher import match_ride


def test_matching_success(db_session, redis_conn, redis_geo):
    rider = Rider(id="rider_1", name="Alice")
    db_session.add(rider)

    driver = Driver(id="driver_1", name="Bob", status="available")
    db_session.add(driver)

    ride = Ride(
        id="ride_100",
        rider_id="rider_1",
        pickup_lat=37.7749,
        pickup_lon=-122.4194,
        status="pending",
    )
    db_session.add(ride)
    db_session.commit()

    redis_conn.set("driver:status:driver_1", "available")
    redis_geo.update_driver_location("driver_1", 37.7750, -122.4190)

    pubsub = redis_conn.pubsub()
    pubsub.subscribe("ride_updates:ride_100")
    pubsub.get_message(timeout=1.0)

    result = match_ride(
        ride_id="ride_100",
        pickup_lat=37.7749,
        pickup_lon=-122.4194,
        db=db_session,
        redis_conn=redis_conn,
        geo=redis_geo,
    )

    assert result["status"] == "matched"
    assert result["driver_id"] == "driver_1"

    db_session.refresh(driver)
    db_session.refresh(ride)
    assert driver.status == "busy"
    assert ride.status == "matched"
    assert ride.driver_id == "driver_1"
    assert ride.matched_at is not None

    assert redis_conn.get("driver:status:driver_1") == "busy"

    msg = pubsub.get_message(timeout=2.0)
    assert msg is not None
    payload = json.loads(msg["data"])
    assert payload["status"] == "matched"
    assert payload["driver_id"] == "driver_1"


def test_matching_no_driver_found(db_session, redis_conn, redis_geo):
    rider = Rider(id="rider_2", name="Charlie")
    db_session.add(rider)
    ride = Ride(
        id="ride_200",
        rider_id="rider_2",
        pickup_lat=37.7749,
        pickup_lon=-122.4194,
        status="pending",
    )
    db_session.add(ride)
    db_session.commit()

    result = match_ride(
        ride_id="ride_200",
        pickup_lat=37.7749,
        pickup_lon=-122.4194,
        db=db_session,
        redis_conn=redis_conn,
        geo=redis_geo,
    )

    assert result["status"] == "no_driver_found"
    assert result["driver_id"] is None

    db_session.refresh(ride)
    assert ride.status == "no_driver_found"

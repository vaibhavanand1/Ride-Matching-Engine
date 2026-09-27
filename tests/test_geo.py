import pytest
from common.geo.redis_geo import RedisGeoBackend
from common.geo.h3_geo import H3GeoBackend


def test_redis_geo_lookup(redis_geo: RedisGeoBackend):
    redis_geo.update_driver_location("drv_1", 37.7749, -122.4194)
    redis_geo.update_driver_location("drv_2", 37.7850, -122.4050)
    redis_geo.update_driver_location("drv_3", 37.8044, -122.2712)

    nearby = redis_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" in nearby
    assert "drv_2" in nearby
    assert "drv_3" not in nearby
    assert nearby[0] == "drv_1"

    redis_geo.remove_driver("drv_1")
    nearby_after = redis_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" not in nearby_after
    assert "drv_2" in nearby_after


def test_h3_geo_lookup(h3_geo: H3GeoBackend):
    h3_geo.update_driver_location("drv_1", 37.7749, -122.4194)
    h3_geo.update_driver_location("drv_2", 37.7780, -122.4150)
    h3_geo.update_driver_location("drv_3", 37.8044, -122.2712)

    nearby = h3_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" in nearby
    assert "drv_2" in nearby
    assert "drv_3" not in nearby
    assert nearby[0] == "drv_1"

    h3_geo.update_driver_location("drv_1", 37.8044, -122.2712)
    nearby_after = h3_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" not in nearby_after

    h3_geo.remove_driver("drv_2")
    assert "drv_2" not in h3_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)

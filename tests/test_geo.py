import pytest
from common.geo.redis_geo import RedisGeoBackend
from common.geo.h3_geo import H3GeoBackend


def test_redis_geo_lookup(redis_geo: RedisGeoBackend):
    # Driver 1 in downtown SF
    redis_geo.update_driver_location("drv_1", 37.7749, -122.4194)
    # Driver 2 slightly further away (~1.5 km)
    redis_geo.update_driver_location("drv_2", 37.7850, -122.4050)
    # Driver 3 far away in Oakland (~12 km)
    redis_geo.update_driver_location("drv_3", 37.8044, -122.2712)

    # Search within 3 km from downtown SF
    nearby = redis_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" in nearby
    assert "drv_2" in nearby
    assert "drv_3" not in nearby

    # Nearest driver should be drv_1
    assert nearby[0] == "drv_1"

    # Remove driver
    redis_geo.remove_driver("drv_1")
    nearby_after = redis_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" not in nearby_after
    assert "drv_2" in nearby_after


def test_h3_geo_lookup(h3_geo: H3GeoBackend):
    # Driver 1 in downtown SF
    h3_geo.update_driver_location("drv_1", 37.7749, -122.4194)
    # Driver 2 close by
    h3_geo.update_driver_location("drv_2", 37.7780, -122.4150)
    # Driver 3 in Oakland (~12 km)
    h3_geo.update_driver_location("drv_3", 37.8044, -122.2712)

    nearby = h3_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" in nearby
    assert "drv_2" in nearby
    assert "drv_3" not in nearby

    # Nearest driver should be drv_1
    assert nearby[0] == "drv_1"

    # Moving driver to Oakland should update cell membership
    h3_geo.update_driver_location("drv_1", 37.8044, -122.2712)
    nearby_after = h3_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)
    assert "drv_1" not in nearby_after

    # Removal cleans up keys
    h3_geo.remove_driver("drv_2")
    assert "drv_2" not in h3_geo.find_nearby_drivers(37.7749, -122.4194, radius_km=3.0)

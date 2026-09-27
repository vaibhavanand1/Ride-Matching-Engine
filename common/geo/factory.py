from typing import List, Optional
import redis
from common.config import GEO_BACKEND, H3_RESOLUTION
from common.geo.base import GeoBackend
from common.geo.redis_geo import RedisGeoBackend
from common.geo.h3_geo import H3GeoBackend
from common.redis_client import get_redis_client


class DualGeoBackend(GeoBackend):
    def __init__(self, redis_backend: RedisGeoBackend, h3_backend: H3GeoBackend):
        self.redis_backend = redis_backend
        self.h3_backend = h3_backend

    def update_driver_location(self, driver_id: str, lat: float, lon: float) -> None:
        self.redis_backend.update_driver_location(driver_id, lat, lon)
        self.h3_backend.update_driver_location(driver_id, lat, lon)

    def find_nearby_drivers(self, lat: float, lon: float, radius_km: float = 5.0, limit: int = 10) -> List[str]:
        return self.redis_backend.find_nearby_drivers(lat, lon, radius_km, limit)

    def remove_driver(self, driver_id: str) -> None:
        self.redis_backend.remove_driver(driver_id)
        self.h3_backend.remove_driver(driver_id)


def get_geo_backend(name: Optional[str] = None, client: Optional[redis.Redis] = None) -> GeoBackend:
    if client is None:
        client = get_redis_client()

    backend_type = (name or GEO_BACKEND).lower()

    if backend_type == "redis":
        return RedisGeoBackend(client)
    elif backend_type == "h3":
        return H3GeoBackend(client, resolution=H3_RESOLUTION)
    elif backend_type == "both":
        return DualGeoBackend(RedisGeoBackend(client), H3GeoBackend(client, resolution=H3_RESOLUTION))
    else:
        raise ValueError(f"Unknown GEO_BACKEND: {backend_type}. Supported: 'redis', 'h3', 'both'")

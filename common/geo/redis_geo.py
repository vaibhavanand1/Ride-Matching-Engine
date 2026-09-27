from typing import List
import redis
from common.geo.base import GeoBackend


class RedisGeoBackend(GeoBackend):
    def __init__(self, client: redis.Redis, key: str = "drivers:geo"):
        self.client = client
        self.key = key

    def update_driver_location(self, driver_id: str, lat: float, lon: float) -> None:
        # Redis GEOADD expects longitude first, then latitude
        self.client.geoadd(self.key, (lon, lat, driver_id))

    def find_nearby_drivers(self, lat: float, lon: float, radius_km: float = 5.0, limit: int = 10) -> List[str]:
        results = self.client.geosearch(
            name=self.key,
            longitude=lon,
            latitude=lat,
            radius=radius_km,
            unit="km",
            sort="ASC",
            count=limit,
        )
        return [str(d) for d in results] if results else []

    def remove_driver(self, driver_id: str) -> None:
        self.client.zrem(self.key, driver_id)

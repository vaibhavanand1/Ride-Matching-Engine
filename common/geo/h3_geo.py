import math
from typing import List, Optional
import redis
import h3
from common.geo.base import GeoBackend


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


class H3GeoBackend(GeoBackend):
    def __init__(self, client: redis.Redis, resolution: int = 8):
        self.client = client
        self.resolution = resolution

    def update_driver_location(self, driver_id: str, lat: float, lon: float) -> None:
        new_cell = h3.latlng_to_cell(lat, lon, self.resolution)
        old_cell = self.client.get(f"driver:cell:{driver_id}")

        pipe = self.client.pipeline()
        if old_cell and old_cell != new_cell:
            pipe.srem(f"h3:{old_cell}", driver_id)

        pipe.sadd(f"h3:{new_cell}", driver_id)
        pipe.set(f"driver:cell:{driver_id}", new_cell)
        pipe.set(f"driver:coords:{driver_id}", f"{lat},{lon}")
        pipe.execute()

    def find_nearby_drivers(self, lat: float, lon: float, radius_km: float = 5.0, limit: int = 10) -> List[str]:
        origin_cell = h3.latlng_to_cell(lat, lon, self.resolution)

        # Res 8 hex diameter is ~0.9km. Step through concentric rings until enough candidates are collected.
        max_k = max(1, int(radius_km / 0.8) + 1)
        candidates: List[str] = []
        seen = set()

        for k in range(max_k + 1):
            if k == 0:
                ring_cells = [origin_cell]
            else:
                try:
                    ring_cells = h3.grid_ring(origin_cell, k)
                except Exception:
                    break

            if not ring_cells:
                continue

            pipe = self.client.pipeline()
            for cell in ring_cells:
                pipe.smembers(f"h3:{cell}")
            results = pipe.execute()

            for members in results:
                for driver_id in members:
                    if driver_id not in seen:
                        seen.add(driver_id)
                        candidates.append(driver_id)

            if len(candidates) >= limit:
                break

        if not candidates:
            return []

        # Sort candidates by exact distance using stored coordinates
        pipe = self.client.pipeline()
        for d in candidates:
            pipe.get(f"driver:coords:{d}")
        coords_list = pipe.execute()

        scored = []
        for d, coords_str in zip(candidates, coords_list):
            if not coords_str:
                scored.append((float("inf"), d))
                continue
            try:
                c_lat, c_lon = map(float, coords_str.split(","))
                dist = haversine_km(lat, lon, c_lat, c_lon)
                if dist <= radius_km:
                    scored.append((dist, d))
            except (ValueError, TypeError):
                continue

        scored.sort(key=lambda x: x[0])
        return [driver_id for _, driver_id in scored[:limit]]

    def remove_driver(self, driver_id: str) -> None:
        cell = self.client.get(f"driver:cell:{driver_id}")
        pipe = self.client.pipeline()
        if cell:
            pipe.srem(f"h3:{cell}", driver_id)
        pipe.delete(f"driver:cell:{driver_id}", f"driver:coords:{driver_id}")
        pipe.execute()

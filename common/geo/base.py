from abc import ABC, abstractmethod
from typing import List


class GeoBackend(ABC):
    @abstractmethod
    def update_driver_location(self, driver_id: str, lat: float, lon: float) -> None:
        """Store or update driver coordinates."""
        pass

    @abstractmethod
    def find_nearby_drivers(self, lat: float, lon: float, radius_km: float = 5.0, limit: int = 10) -> List[str]:
        """Find driver IDs within radius_km sorted nearest to farthest."""
        pass

    @abstractmethod
    def remove_driver(self, driver_id: str) -> None:
        """Remove driver from spatial index."""
        pass

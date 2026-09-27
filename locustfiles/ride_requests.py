import os
import random
import uuid
from locust import HttpUser, task, between

LAT_MIN, LAT_MAX = 37.685, 37.865
LON_MIN, LON_MAX = -122.529, -122.309

LOCUST_GEO_BACKEND = os.getenv("LOCUST_GEO_BACKEND", "").lower()


class RiderUser(HttpUser):
    wait_time = between(0.05, 0.2)

    def on_start(self):
        self.rider_id = f"rider_{uuid.uuid4().hex[:10]}"

    @task
    def request_ride(self):
        pickup_lat = random.uniform(LAT_MIN, LAT_MAX)
        pickup_lon = random.uniform(LON_MIN, LON_MAX)

        payload = {
            "rider_id": self.rider_id,
            "pickup_lat": pickup_lat,
            "pickup_lon": pickup_lon,
        }

        url = f"/rides/request?backend={LOCUST_GEO_BACKEND}" if LOCUST_GEO_BACKEND else "/rides/request"
        with self.client.post(
            url,
            json=payload,
            name="/rides/request",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 201):
                resp.success()
            else:
                resp.failure(f"Status {resp.status_code}: {resp.text}")

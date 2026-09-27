import time
import grpc
from proto import driver_pb2, driver_pb2_grpc


class DriverTrackingServicer(driver_pb2_grpc.DriverTrackingServicer):
    def __init__(self, geo_backend, redis_client):
        self.geo = geo_backend
        self.redis = redis_client

    async def StreamLocation(self, request_iterator, context):
        async for update in request_iterator:
            driver_id = update.driver_id
            lat = update.latitude
            lon = update.longitude
            now_ts = int(time.time())

            self.geo.update_driver_location(driver_id, lat, lon)

            pipe = self.redis.pipeline()
            pipe.set(f"driver:last_seen:{driver_id}", now_ts)
            # Default to available if not explicitly set
            pipe.set(f"driver:status:{driver_id}", "available", nx=True)
            pipe.execute()

            yield driver_pb2.LocationResponse(
                driver_id=driver_id,
                acknowledged=True,
                message="ok",
                server_timestamp=now_ts,
            )


async def start_grpc_server(geo_backend, redis_client, host: str = "0.0.0.0", port: int = 50051):
    server = grpc.aio.server()
    driver_pb2_grpc.add_DriverTrackingServicer_to_server(
        DriverTrackingServicer(geo_backend, redis_client), server
    )
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    return server

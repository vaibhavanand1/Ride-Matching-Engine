import time
import grpc
from proto import driver_pb2, driver_pb2_grpc
from common.config import KAFKA_TOPIC_DRIVER_TELEMETRY


class DriverTrackingServicer(driver_pb2_grpc.DriverTrackingServicer):
    def __init__(self, geo_backend, redis_client, kafka_producer=None):
        self.geo = geo_backend
        self.redis = redis_client
        self.kafka_producer = kafka_producer

    async def StreamLocation(self, request_iterator, context):
        async for update in request_iterator:
            driver_id = update.driver_id
            lat = update.latitude
            lon = update.longitude
            now_ts = int(time.time())

            payload = {
                "driver_id": driver_id,
                "latitude": lat,
                "longitude": lon,
                "timestamp": now_ts,
            }

            # Produce to partitioned Kafka topic (keyed by driver_id) if connected
            if self.kafka_producer and self.kafka_producer.is_connected:
                await self.kafka_producer.send(
                    topic=KAFKA_TOPIC_DRIVER_TELEMETRY,
                    key=driver_id,
                    value=payload,
                )
            else:
                # Direct fallback for tests or standalone mode
                self.geo.update_driver_location(driver_id, lat, lon)
                pipe = self.redis.pipeline()
                pipe.set(f"driver:last_seen:{driver_id}", now_ts)
                pipe.set(f"driver:status:{driver_id}", "available", nx=True)
                pipe.execute()

            yield driver_pb2.LocationResponse(
                driver_id=driver_id,
                acknowledged=True,
                message="ok",
                server_timestamp=now_ts,
            )


async def start_grpc_server(
    geo_backend,
    redis_client,
    kafka_producer=None,
    host: str = "0.0.0.0",
    port: int = 50051,
):
    server = grpc.aio.server()
    driver_pb2_grpc.add_DriverTrackingServicer_to_server(
        DriverTrackingServicer(geo_backend, redis_client, kafka_producer), server
    )
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    return server

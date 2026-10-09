import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from common.config import (
    GEO_BACKEND,
    KAFKA_TOPIC_RIDE_REQUESTS,
    KAFKA_TOPIC_RIDE_EVENTS,
    KAFKA_CONSUMER_GROUP_MATCHING,
)
from common.database import get_db, init_db, SessionLocal
from common.redis_client import get_redis_client
from common.geo.factory import get_geo_backend
from common.kafka_client import get_kafka_producer, create_kafka_consumer
from matching_service.matcher import match_ride

logger = logging.getLogger(__name__)


class MatchRequest(BaseModel):
    ride_id: str
    pickup_lat: float
    pickup_lon: float
    radius_km: float = Field(default=5.0, gt=0)


class MatchResponse(BaseModel):
    status: str
    ride_id: str
    driver_id: Optional[str] = None
    matched_at: Optional[str] = None


redis_conn = get_redis_client()
geo_backend = get_geo_backend(client=redis_conn)
kafka_producer = get_kafka_producer()
matching_consumer_task = None


async def run_matching_worker():
    """Background worker consuming ride-requests from Kafka with offset tracking."""
    consumer = None
    try:
        consumer = create_kafka_consumer(
            KAFKA_TOPIC_RIDE_REQUESTS,
            group_id=KAFKA_CONSUMER_GROUP_MATCHING,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        logger.info("Matching worker active, listening on %s", KAFKA_TOPIC_RIDE_REQUESTS)
        async for msg in consumer:
            payload = msg.value
            if isinstance(payload, dict):
                ride_id = payload.get("ride_id")
                lat = payload.get("pickup_lat")
                lon = payload.get("pickup_lon")
                radius = payload.get("radius_km", 5.0)
                backend_override = payload.get("backend")

                if ride_id and lat is not None and lon is not None:
                    with SessionLocal() as db:
                        active_geo = (
                            get_geo_backend(backend_override, client=redis_conn)
                            if backend_override
                            else geo_backend
                        )
                        result = match_ride(
                            ride_id=ride_id,
                            pickup_lat=lat,
                            pickup_lon=lon,
                            db=db,
                            redis_conn=redis_conn,
                            geo=active_geo,
                            radius_km=radius,
                        )

                        # Publish match event to Kafka topic ride-events
                        if kafka_producer.is_connected:
                            await kafka_producer.send(
                                topic=KAFKA_TOPIC_RIDE_EVENTS,
                                key=ride_id,
                                value=result,
                            )

            # Commit offset after successful handling
            await consumer.commit()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning("Matching consumer worker error or Kafka unavailable: %s", e)
    finally:
        if consumer:
            try:
                await consumer.stop()
            except Exception:
                pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global matching_consumer_task
    init_db()
    await kafka_producer.start()
    matching_consumer_task = asyncio.create_task(run_matching_worker())
    yield
    if matching_consumer_task:
        matching_consumer_task.cancel()
        try:
            await matching_consumer_task
        except (asyncio.CancelledError, Exception):
            pass
    await kafka_producer.stop()


app = FastAPI(title="Matching Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "matching-service",
        "geo_backend": GEO_BACKEND,
        "kafka_connected": kafka_producer.is_connected,
    }


@app.post("/matches/dispatch", response_model=MatchResponse)
async def dispatch_match(
    payload: MatchRequest,
    backend: Optional[str] = None,
    db: Session = Depends(get_db),
):
    active_geo = get_geo_backend(backend, client=redis_conn) if backend else geo_backend
    result = match_ride(
        ride_id=payload.ride_id,
        pickup_lat=payload.pickup_lat,
        pickup_lon=payload.pickup_lon,
        db=db,
        redis_conn=redis_conn,
        geo=active_geo,
        radius_km=payload.radius_km,
    )

    if kafka_producer.is_connected:
        await kafka_producer.send(
            topic=KAFKA_TOPIC_RIDE_EVENTS,
            key=payload.ride_id,
            value=result,
        )

    return MatchResponse(**result)

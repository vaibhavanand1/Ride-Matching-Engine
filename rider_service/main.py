import uuid
import logging
from typing import Optional
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from common.config import MATCHING_SERVICE_URL, KAFKA_TOPIC_RIDE_REQUESTS
from common.database import get_db, init_db
from common.models import Rider, Ride
from common.kafka_client import get_kafka_producer

logger = logging.getLogger(__name__)
kafka_producer = get_kafka_producer()


class RiderCreate(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str


class RiderResponse(BaseModel):
    id: str
    name: str


class RideRequest(BaseModel):
    rider_id: str
    pickup_lat: float = Field(..., ge=-90.0, le=90.0)
    pickup_lon: float = Field(..., ge=-180.0, le=180.0)


class RideResponse(BaseModel):
    id: str
    rider_id: str
    driver_id: Optional[str] = None
    pickup_lat: float
    pickup_lon: float
    status: str
    requested_at: Optional[str] = None
    matched_at: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await kafka_producer.start()
    yield
    await kafka_producer.stop()


app = FastAPI(title="Rider Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "rider-service",
        "kafka_connected": kafka_producer.is_connected,
    }


@app.post("/riders", response_model=RiderResponse, status_code=status.HTTP_201_CREATED)
def register_rider(payload: RiderCreate, db: Session = Depends(get_db)):
    existing = db.query(Rider).filter(Rider.id == payload.id).first()
    if existing:
        return RiderResponse(id=existing.id, name=existing.name)

    rider = Rider(id=payload.id, name=payload.name)
    db.add(rider)
    db.commit()
    db.refresh(rider)
    return RiderResponse(id=rider.id, name=rider.name)


@app.post("/rides/request", response_model=RideResponse, status_code=status.HTTP_201_CREATED)
async def request_ride(
    payload: RideRequest,
    backend: Optional[str] = None,
    sync: bool = False,
    db: Session = Depends(get_db),
):
    rider = db.query(Rider).filter(Rider.id == payload.rider_id).first()
    if not rider:
        rider = Rider(id=payload.rider_id, name=f"Rider-{payload.rider_id[:8]}")
        db.add(rider)
        db.commit()

    ride_id = str(uuid.uuid4())
    ride = Ride(
        id=ride_id,
        rider_id=payload.rider_id,
        pickup_lat=payload.pickup_lat,
        pickup_lon=payload.pickup_lon,
        status="pending",
    )
    db.add(ride)
    db.commit()
    db.refresh(ride)

    # 1. Primary event-driven path: publish to Kafka topic ride-requests
    if kafka_producer.is_connected and not sync:
        await kafka_producer.send(
            topic=KAFKA_TOPIC_RIDE_REQUESTS,
            key=ride_id,
            value={
                "ride_id": ride_id,
                "rider_id": payload.rider_id,
                "pickup_lat": payload.pickup_lat,
                "pickup_lon": payload.pickup_lon,
                "backend": backend,
            },
        )
    else:
        # 2. Synchronous fallback (when ?sync=true or Kafka offline)
        try:
            params = {"backend": backend} if backend else {}
            async with httpx.AsyncClient(timeout=4.0) as client:
                resp = await client.post(
                    f"{MATCHING_SERVICE_URL}/matches/dispatch",
                    params=params,
                    json={
                        "ride_id": ride_id,
                        "pickup_lat": payload.pickup_lat,
                        "pickup_lon": payload.pickup_lon,
                    },
                )
                if resp.status_code == 200:
                    db.refresh(ride)
                else:
                    ride.status = "no_driver_found"
                    db.commit()
        except Exception as e:
            logger.warning("Synchronous matching dispatch failed: %s", e)

    return RideResponse(
        id=ride.id,
        rider_id=ride.rider_id,
        driver_id=ride.driver_id,
        pickup_lat=ride.pickup_lat,
        pickup_lon=ride.pickup_lon,
        status=ride.status,
        requested_at=ride.requested_at.isoformat() if ride.requested_at else None,
        matched_at=ride.matched_at.isoformat() if ride.matched_at else None,
    )


@app.get("/rides/{ride_id}", response_model=RideResponse)
def get_ride(ride_id: str, db: Session = Depends(get_db)):
    ride = db.query(Ride).filter(Ride.id == ride_id).first()
    if not ride:
        raise HTTPException(status_code=404, detail="Ride not found")

    return RideResponse(
        id=ride.id,
        rider_id=ride.rider_id,
        driver_id=ride.driver_id,
        pickup_lat=ride.pickup_lat,
        pickup_lon=ride.pickup_lon,
        status=ride.status,
        requested_at=ride.requested_at.isoformat() if ride.requested_at else None,
        matched_at=ride.matched_at.isoformat() if ride.matched_at else None,
    )

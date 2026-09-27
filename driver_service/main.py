import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from common.config import (
    DRIVER_SERVICE_GRPC_PORT,
    GEO_BACKEND,
)
from common.database import get_db, init_db
from common.models import Driver
from common.redis_client import get_redis_client
from common.geo.factory import get_geo_backend
from driver_service.grpc_server import start_grpc_server


class DriverCreate(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    status: str = "available"


class DriverStatusUpdate(BaseModel):
    status: str = Field(..., pattern="^(available|busy|offline)$")


class DriverResponse(BaseModel):
    id: str
    name: str
    status: str


redis_conn = get_redis_client()
geo_backend = get_geo_backend(client=redis_conn)
grpc_server_instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global grpc_server_instance
    init_db()
    grpc_server_instance = await start_grpc_server(
        geo_backend, redis_conn, port=DRIVER_SERVICE_GRPC_PORT
    )
    yield
    if grpc_server_instance:
        await grpc_server_instance.stop(grace=3.0)


app = FastAPI(title="Driver Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "driver-service", "geo_backend": GEO_BACKEND}


@app.post("/drivers", response_model=DriverResponse, status_code=status.HTTP_201_CREATED)
def register_driver(payload: DriverCreate, db: Session = Depends(get_db)):
    existing = db.query(Driver).filter(Driver.id == payload.id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Driver ID already exists")

    driver = Driver(id=payload.id, name=payload.name, status=payload.status)
    db.add(driver)
    db.commit()
    db.refresh(driver)

    redis_conn.set(f"driver:status:{driver.id}", driver.status)
    return DriverResponse(id=driver.id, name=driver.name, status=driver.status)


@app.patch("/drivers/{driver_id}/status", response_model=DriverResponse)
def update_driver_status(
    driver_id: str, payload: DriverStatusUpdate, db: Session = Depends(get_db)
):
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.status = payload.status
    db.commit()

    redis_conn.set(f"driver:status:{driver_id}", payload.status)
    if payload.status == "offline":
        geo_backend.remove_driver(driver_id)

    return DriverResponse(id=driver.id, name=driver.name, status=driver.status)


@app.get("/drivers/{driver_id}", response_model=DriverResponse)
def get_driver(driver_id: str, db: Session = Depends(get_db)):
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    live_status = redis_conn.get(f"driver:status:{driver_id}") or driver.status
    return DriverResponse(id=driver.id, name=driver.name, status=live_status)

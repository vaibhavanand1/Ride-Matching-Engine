from fastapi import FastAPI, Depends
from pydantic import BaseModel, Field
from typing import Optional
from sqlalchemy.orm import Session

from common.config import GEO_BACKEND
from common.database import get_db, init_db
from common.redis_client import get_redis_client
from common.geo.factory import get_geo_backend
from matching_service.matcher import match_ride


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

app = FastAPI(title="Matching Service")


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok", "service": "matching-service", "geo_backend": GEO_BACKEND}


@app.post("/matches/dispatch", response_model=MatchResponse)
def dispatch_match(
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
    return MatchResponse(**result)

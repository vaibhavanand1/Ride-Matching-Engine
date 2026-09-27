import pytest
from sqlalchemy.orm import Session

from common.database import SessionLocal, init_db, engine, Base
from common.redis_client import get_redis_client
from common.models import Driver, Rider, Ride
from common.geo.redis_geo import RedisGeoBackend
from common.geo.h3_geo import H3GeoBackend


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    init_db()
    yield


@pytest.fixture
def redis_conn():
    client = get_redis_client()
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def db_session():
    session: Session = SessionLocal()
    # Clean tables
    session.query(Ride).delete()
    session.query(Driver).delete()
    session.query(Rider).delete()
    session.commit()
    yield session
    session.query(Ride).delete()
    session.query(Driver).delete()
    session.query(Rider).delete()
    session.commit()
    session.close()


@pytest.fixture
def redis_geo(redis_conn):
    return RedisGeoBackend(redis_conn)


@pytest.fixture
def h3_geo(redis_conn):
    return H3GeoBackend(redis_conn, resolution=8)

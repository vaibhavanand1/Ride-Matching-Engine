from datetime import datetime, timezone
from sqlalchemy import Column, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from common.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class Driver(Base):
    __tablename__ = "drivers"

    id = Column(String(64), primary_key=True, index=True)
    name = Column(String(128), nullable=False)
    status = Column(String(32), nullable=False, default="offline", index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    rides = relationship("Ride", back_populates="driver")


class Rider(Base):
    __tablename__ = "riders"

    id = Column(String(64), primary_key=True, index=True)
    name = Column(String(128), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    rides = relationship("Ride", back_populates="rider")


class Ride(Base):
    __tablename__ = "rides"

    id = Column(String(64), primary_key=True, index=True)
    rider_id = Column(String(64), ForeignKey("riders.id"), nullable=False, index=True)
    driver_id = Column(String(64), ForeignKey("drivers.id"), nullable=True, index=True)
    pickup_lat = Column(Float, nullable=False)
    pickup_lon = Column(Float, nullable=False)
    status = Column(String(32), nullable=False, default="pending", index=True)
    requested_at = Column(DateTime(timezone=True), default=utcnow)
    matched_at = Column(DateTime(timezone=True), nullable=True)

    rider = relationship("Rider", back_populates="rides")
    driver = relationship("Driver", back_populates="rides")

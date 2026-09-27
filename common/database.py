from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from common.config import DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=40,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from common import models  # noqa
    Base.metadata.create_all(bind=engine)

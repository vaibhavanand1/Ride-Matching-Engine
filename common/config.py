import os

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "rides_db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

GEO_BACKEND = os.getenv("GEO_BACKEND", "redis").lower()
H3_RESOLUTION = int(os.getenv("H3_RESOLUTION", "8"))

MATCHING_SERVICE_URL = os.getenv("MATCHING_SERVICE_URL", "http://localhost:8002")
DRIVER_SERVICE_GRPC_TARGET = os.getenv("DRIVER_SERVICE_GRPC_TARGET", "localhost:50051")

DRIVER_SERVICE_PORT = int(os.getenv("DRIVER_SERVICE_PORT", "8001"))
DRIVER_SERVICE_GRPC_PORT = int(os.getenv("DRIVER_SERVICE_GRPC_PORT", "50051"))
RIDER_SERVICE_PORT = int(os.getenv("RIDER_SERVICE_PORT", "8000"))
MATCHING_SERVICE_PORT = int(os.getenv("MATCHING_SERVICE_PORT", "8002"))

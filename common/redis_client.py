import uuid
import redis
from common.config import REDIS_HOST, REDIS_PORT

LUA_RELEASE_LOCK = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
    )


class DriverLock:
    def __init__(self, client: redis.Redis, driver_id: str, ttl: int = 5):
        self.client = client
        self.driver_id = driver_id
        self.ttl = ttl
        self.key = f"lock:driver:{driver_id}"
        self.token = str(uuid.uuid4())
        self.acquired = False
        self._release_script = self.client.register_script(LUA_RELEASE_LOCK)

    def acquire(self) -> bool:
        self.acquired = bool(self.client.set(self.key, self.token, nx=True, ex=self.ttl))
        return self.acquired

    def release(self):
        if not self.acquired:
            return
        try:
            self._release_script(keys=[self.key], args=[self.token])
        finally:
            self.acquired = False

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(f"Could not acquire lock for driver {self.driver_id}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

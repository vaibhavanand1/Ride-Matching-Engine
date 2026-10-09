import json
import logging
from typing import Any, Optional, Union
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from common.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)


def json_serializer(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return json.dumps(value).encode("utf-8")


def key_serializer(key: Optional[Union[str, bytes]]) -> Optional[bytes]:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key
    return str(key).encode("utf-8")


def json_deserializer(value: bytes) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except Exception:
        return value.decode("utf-8", errors="ignore")


def key_deserializer(key: Optional[bytes]) -> Optional[str]:
    if key is None:
        return None
    return key.decode("utf-8", errors="ignore")


class KafkaProducerManager:
    """Async Kafka Producer lifecycle manager with JSON serialization."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None
        self._is_started = False

    async def start(self) -> bool:
        if self._is_started and self._producer:
            return True
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=json_serializer,
                key_serializer=key_serializer,
                request_timeout_ms=5000,
            )
            await self._producer.start()
            self._is_started = True
            logger.info("Kafka producer connected to %s", self.bootstrap_servers)
            return True
        except Exception as e:
            logger.warning("Failed to start Kafka producer (%s): %s", self.bootstrap_servers, e)
            self._producer = None
            self._is_started = False
            return False

    async def stop(self):
        if self._producer and self._is_started:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning("Error stopping Kafka producer: %s", e)
            finally:
                self._producer = None
                self._is_started = False

    async def send(self, topic: str, value: Any, key: Optional[Union[str, bytes]] = None):
        """Send message to Kafka topic. If Kafka is unavailable, logs warning."""
        if not self._is_started or not self._producer:
            logger.debug("Kafka producer not active. Message to '%s' skipped.", topic)
            return None
        return await self._producer.send_and_wait(topic=topic, value=value, key=key)

    @property
    def is_connected(self) -> bool:
        return self._is_started and self._producer is not None


_global_producer: Optional[KafkaProducerManager] = None


def get_kafka_producer(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> KafkaProducerManager:
    global _global_producer
    if _global_producer is None:
        _global_producer = KafkaProducerManager(bootstrap_servers=bootstrap_servers)
    return _global_producer


def create_kafka_consumer(
    *topics: str,
    group_id: str,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset: str = "latest",
    enable_auto_commit: bool = True,
) -> AIOKafkaConsumer:
    """Create a configured AIOKafkaConsumer with JSON deserialization."""
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        value_deserializer=json_deserializer,
        key_deserializer=key_deserializer,
    )

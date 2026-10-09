import pytest
from common.kafka_client import (
    json_serializer,
    json_deserializer,
    key_serializer,
    key_deserializer,
    KafkaProducerManager,
)


def test_kafka_serialization():
    payload = {"driver_id": "drv_123", "latitude": 37.7749, "longitude": -122.4194}
    serialized = json_serializer(payload)
    assert isinstance(serialized, bytes)

    deserialized = json_deserializer(serialized)
    assert deserialized == payload
    assert deserialized["driver_id"] == "drv_123"


def test_kafka_key_serialization():
    key_str = "driver_456"
    encoded = key_serializer(key_str)
    assert encoded == b"driver_456"
    assert key_deserializer(encoded) == "driver_456"

    assert key_serializer(None) is None
    assert key_deserializer(None) is None


def test_kafka_producer_graceful_when_not_started():
    manager = KafkaProducerManager(bootstrap_servers="nonexistent:9092")
    assert not manager.is_connected

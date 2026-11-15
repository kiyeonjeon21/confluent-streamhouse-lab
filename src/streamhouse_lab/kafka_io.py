"""Kafka clients for Phase 4: a producer for generated records, topic readers, and truncation.

Serializers use the latest registered schema for each subject and never
register one (`use.latest.version`, no auto-registration), so a mismatch fails
loudly instead of forking the schema that Flink, Lightning, and Tableflow read.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_END,
    Consumer,
    KafkaError,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streamhouse_lab.config import ConfigError, LabConfig
from streamhouse_lab.datagen import Record

SERIALIZER_CONF = {"auto.register.schemas": False, "use.latest.version": True}


class ProduceError(RuntimeError):
    pass


@dataclass
class Ack:
    topic: str
    partition: int
    offset: int
    acked_ns: int
    # Broker-side record timestamp in epoch ms (CreateTime unless the topic uses LogAppendTime).
    timestamp_ms: int | None = None


MISSING_KEYS = "Set enable_bench_clients = true in terraform.tfvars and redeploy."


def client_config(config: LabConfig) -> dict:
    if config.operator_kafka is None or not config.kafka_bootstrap:
        raise ConfigError(f"No Kafka client key. {MISSING_KEYS}")
    return {
        # Terraform reports the endpoint with its protocol, e.g. SASL_SSL://host:9092.
        "bootstrap.servers": config.kafka_bootstrap.split("://", 1)[-1],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": config.operator_kafka.key,
        "sasl.password": config.operator_kafka.secret.reveal(),
        # librdkafka logs telemetry and connection details at info (6); keep warnings and above.
        "log_level": 4,
    }


def producer_config(config: LabConfig) -> dict:
    return {**client_config(config), "acks": "all", "enable.idempotence": True, "linger.ms": 5}


def consumer_config(config: LabConfig, isolation: str) -> dict:
    if isolation not in ("read_committed", "read_uncommitted"):
        raise ValueError(f"unknown isolation level {isolation!r}")
    return {
        **client_config(config),
        # A fresh group per reader; offsets are assigned explicitly and never committed.
        "group.id": f"lab-reader-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "isolation.level": isolation,
    }


def registry_config(config: LabConfig) -> dict:
    if config.operator_schema_registry is None or not config.schema_registry_endpoint:
        raise ConfigError(f"No Schema Registry key. {MISSING_KEYS}")
    key = config.operator_schema_registry
    return {
        "url": config.schema_registry_endpoint,
        "basic.auth.user.info": f"{key.key}:{key.secret.reveal()}",
    }


class BenchProducer:
    def __init__(
        self,
        producer: Producer,
        serialize: Callable[[Record], tuple[bytes, bytes]],
        clock: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._producer = producer
        self._serialize = serialize
        self._clock = clock

    @classmethod
    def connect(cls, config: LabConfig) -> BenchProducer:
        registry = SchemaRegistryClient(registry_config(config))
        serializers: dict[tuple[str, MessageField], AvroSerializer] = {}

        def serialize(record: Record) -> tuple[bytes, bytes]:
            out = []
            for field, payload in ((MessageField.KEY, record.key), (MessageField.VALUE, record.value)):
                serializer = serializers.get((record.topic, field))
                if serializer is None:
                    serializer = AvroSerializer(registry, None, conf=SERIALIZER_CONF)
                    serializers[(record.topic, field)] = serializer
                out.append(serializer(payload, SerializationContext(record.topic, field)))
            return out[0], out[1]

        return cls(Producer(producer_config(config)), serialize)

    def send(self, records: Iterable[Record]) -> list[Ack]:
        """Produce records in order and wait for every acknowledgement."""
        acks: list[Ack] = []
        failures: list[str] = []

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                failures.append(str(err))
            else:
                acks.append(
                    Ack(msg.topic(), msg.partition(), msg.offset(), self._clock(), msg.timestamp()[1])
                )

        for record in records:
            key, value = self._serialize(record)
            while True:
                try:
                    self._producer.produce(record.topic, key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:
                    self._producer.poll(0.1)
            self._producer.poll(0)
        remaining = self._producer.flush(60)
        if failures or remaining:
            raise ProduceError(f"{len(failures)} failed, {remaining} unacknowledged: {failures[:3]}")
        return acks

    def send_one(self, record: Record) -> Ack:
        return self.send([record])[0]


@dataclass
class Received:
    key: Any
    value: Any
    partition: int
    offset: int
    timestamp_ms: int
    received_ns: int


class TopicReader:
    """Reads one topic from explicitly assigned offsets, deserializing Avro keys and values."""

    def __init__(
        self,
        consumer: Consumer,
        deserialize: Callable[[Message], tuple[Any, Any]],
        clock: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._consumer = consumer
        self._deserialize = deserialize
        self._clock = clock

    @classmethod
    def connect(
        cls, config: LabConfig, topic: str, *, isolation: str, from_beginning: bool = False
    ) -> TopicReader:
        consumer = Consumer(consumer_config(config, isolation))
        partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        start = OFFSET_BEGINNING if from_beginning else OFFSET_END
        consumer.assign([TopicPartition(topic, p, start) for p in partitions])
        registry = SchemaRegistryClient(registry_config(config))
        key_reader, value_reader = AvroDeserializer(registry), AvroDeserializer(registry)

        def deserialize(msg: Message) -> tuple[Any, Any]:
            key = msg.key()
            value = msg.value()
            return (
                key_reader(key, SerializationContext(topic, MessageField.KEY)) if key else None,
                value_reader(value, SerializationContext(topic, MessageField.VALUE)) if value else None,
            )

        return cls(consumer, deserialize)

    def poll(self, timeout: float = 0.1, max_messages: int = 500) -> list[Received]:
        out: list[Received] = []
        for msg in self._consumer.consume(max_messages, timeout):
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise ProduceError(f"consume failed: {msg.error()}")
            received = self._clock()
            key, value = self._deserialize(msg)
            out.append(Received(key, value, msg.partition(), msg.offset(), msg.timestamp()[1], received))
        return out

    def watermarks(self) -> dict[int, tuple[int, int]]:
        return {
            tp.partition: self._consumer.get_watermark_offsets(tp, timeout=30)
            for tp in self._consumer.assignment()
        }

    def close(self) -> None:
        self._consumer.close()


def read_all(config: LabConfig, topic: str, *, isolation: str = "read_committed") -> list[Received]:
    """Every record currently retained in the topic, from the log start to the high watermark."""
    reader = TopicReader.connect(config, topic, isolation=isolation, from_beginning=True)
    try:
        ends = {p: high for p, (_low, high) in reader.watermarks().items()}
        positions = {p: low for p, (low, _high) in reader.watermarks().items()}
        records: list[Received] = []
        deadline = time.monotonic() + 60
        while any(positions[p] < ends[p] for p in ends) and time.monotonic() < deadline:
            for rec in reader.poll(0.5):
                records.append(rec)
                positions[rec.partition] = rec.offset + 1
            # Transaction markers occupy offsets but are never delivered; trust the position API.
            for tp in reader._consumer.position([TopicPartition(topic, p) for p in ends]):
                if tp.offset >= 0:
                    positions[tp.partition] = max(positions[tp.partition], tp.offset)
        return records
    finally:
        reader.close()


def truncate(config: LabConfig, topic: str) -> dict[int, int]:
    """Delete every record currently in the topic (Kafka DeleteRecords), as retention would.

    Returns the new log start offset per partition. Downstream copies, such as a
    Tableflow Iceberg table, keep the data; only the Kafka log loses it.
    """
    admin = AdminClient(client_config(config))
    metadata = admin.list_topics(topic, timeout=30).topics[topic]
    futures = admin.delete_records([TopicPartition(topic, p, OFFSET_END) for p in metadata.partitions])
    return {tp.partition: future.result().offset for tp, future in futures.items()}

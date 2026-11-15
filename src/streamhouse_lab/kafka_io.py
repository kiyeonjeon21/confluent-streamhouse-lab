"""Produces generated records to the bench_ topics with the schemas Flink registered.

Serializers use the latest registered schema for each subject and never
register one (`use.latest.version`, no auto-registration), so a mismatch fails
loudly instead of forking the schema that Flink, Lightning, and Tableflow read.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
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


MISSING_KEYS = "Set enable_bench_clients = true in terraform.tfvars and redeploy."


def producer_config(config: LabConfig) -> dict:
    if config.operator_kafka is None or not config.kafka_bootstrap:
        raise ConfigError(f"No Kafka client key. {MISSING_KEYS}")
    return {
        # Terraform reports the endpoint with its protocol, e.g. SASL_SSL://host:9092.
        "bootstrap.servers": config.kafka_bootstrap.split("://", 1)[-1],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": config.operator_kafka.key,
        "sasl.password": config.operator_kafka.secret.reveal(),
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        # librdkafka logs telemetry and connection details at info (6); keep warnings and above.
        "log_level": 4,
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
                acks.append(Ack(msg.topic(), msg.partition(), msg.offset(), self._clock()))

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

import asyncio
import json
from uuid import uuid4
from loguru import logger
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, ConsumerRecord
from typing import Type, Any, Union

from src.consumer.interface import MessageHandler
from src.config.environment import KAFKA_BROKERS, APP_NAME

from src.common.event_source.event_source_topic import EventData


class KafkaAdapter:
    _producer: AIOKafkaProducer | None = None
    _consumer: AIOKafkaConsumer | None = None

    def __init__(self) -> None:
        pass

    # --- Producer ---
    @classmethod
    async def connectProducer(cls) -> None:
        logger.info("Connecting Kafka producer...")
        if cls._producer is None:
            cls._producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                client_id=str(uuid4()),
                retry_backoff_ms=1000,
            )
        await cls._producer.start()
        logger.info("✅ Connected to Kafka producer.")

    @classmethod
    async def disconnectProducer(cls) -> None:
        logger.info("Disconnecting Kafka producer...")
        if cls._producer is not None:
            await cls._producer.stop()
            cls._producer = None
            logger.info("🛑 Kafka producer disconnected")

    @classmethod
    async def send(cls, topic: str, msg: dict[str, Any]):
        if not cls._producer:
            raise RuntimeError("Kafka producer not connected.")
        headers = {
            "id": msg['id'],
            "event": msg['event']
        }

        await cls._producer.send_and_wait(
            topic,
            msg,
            headers=[(k, v.encode("utf-8")) for k, v in headers.items()],
        )
        logger.info(f"📤 Sent message to '{topic}': {msg}")

    # --- Consumer ---
    @classmethod
    async def connectConsumer(cls, handlers: list[Type[MessageHandler]]) -> None:
        topics: list[str] = [h.get_topic().strip() for h in handlers if h.get_topic().strip()]

        cls._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=APP_NAME,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000
        )

        await cls._consumer.start()
        logger.info(f"🎧 Listening to topics: {topics}")

        try:
            async for msg in cls._consumer:
                logger.info(f"📩 Received from {msg.topic}: {msg.value}")
                for handler in handlers:
                    if handler.match(msg.topic, msg):
                        try:
                            parsed_msg = EventData.parse_event_data(msg)
                            await handler.process(msg.topic, msg.partition, msg, parsed_msg)
                        except Exception as e:
                            logger.error(f"❌ Error processing {msg.topic}: {e}")
        finally:
            await cls._consumer.stop()
            logger.info("🛑 Kafka consumer stopped.")

import asyncio
from typing import Type
from datetime import datetime, UTC
from uuid import uuid4

from common.infrastructure.kafka_adapter import KafkaAdapter
from consumer.interface import MessageHandler
from consumer.handlers.user import UserHandler
from common.event_source.event_source_topic import EventData

async def handle_message(data):
    print("✅ Received:", data)

async def main():

    # Khởi chạy producer
    await KafkaAdapter.connectProducer()
    handlers: list[Type[MessageHandler]] = [UserHandler]
    # consumer_task = asyncio.create_task(KafkaAdapter.connectConsumer(handlers))
    # await KafkaAdapter.connectConsumer(handlers)
    msg = EventData.new_from({
        "id": str(uuid4()),                   # tương đương ObjectId().toHexString() bên Node
        "event": "register",
        "topic": "user",
        "key": "complete_lesson",
        "subject": {
            "id": "123",
            "type": "user"
        },
        "di_obj": {
            "data": {
                "country": "VN",
                "lesson_id": "abc"
            }
        },
        "in_obj": None,
        "pr_obj": {
            "id": "roadmap_001",
            "name": "Python Roadmap"
        },
        "context": {
            "source": "test_script",
            "trace_id": str(uuid4())
        },
        "sent_at": int(datetime.now(UTC).timestamp() * 1000)
    }).transform()

    await KafkaAdapter.send("user", msg)

    # Đợi consumer chạy (có thể stop sau vài giây)
    # await asyncio.sleep(10)
    
    # consumer_task.cancel()


asyncio.run(main())



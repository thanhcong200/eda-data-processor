import asyncio
from typing import Type
from datetime import datetime, UTC
from uuid import uuid4

from src.common.infrastructure.kafka_adapter import KafkaAdapter
from src.consumer.interface import MessageHandler
from src.consumer.handlers.user import UserHandler
from src.common.event_source.event_source_topic import EventData
from src.common.queue.queue import app, send_task

async def handle_message(data):
    print("✅ Received:", data)

async def main():

    # Khởi chạy producer
    await KafkaAdapter.connectProducer()
    handlers: list[Type[MessageHandler]] = [UserHandler]
    # consumer_task = asyncio.create_task(KafkaAdapter.connectConsumer(handlers))
    await KafkaAdapter.connectConsumer(handlers)
    
    
    
    
    # msg = EventData.new_from({
    #     "id": str(uuid4()),                   # tương đương ObjectId().toHexString() bên Node
    #     "event": "register",
    #     "topic": "user",
    #     "key": "complete_lesson",
    #     "subject": {
    #         "id": "123",
    #         "type": "user"
    #     },
    #     "di_obj": {
    #         "data": {
    #             "country": "VN",
    #             "lesson_id": "abc"
    #         }
    #     },
    #     "in_obj": None,
    #     "pr_obj": {
    #         "id": "roadmap_001",
    #         "name": "Python Roadmap"
    #     },
    #     "context": {
    #         "source": "test_script",
    #         "trace_id": str(uuid4())
    #     },
    #     "sent_at": int(datetime.now(UTC).timestamp() * 1000)
    # }).transform()

    # await KafkaAdapter.send("user", msg)

    # # Đợi consumer chạy (có thể stop sau vài giây)
    # # await asyncio.sleep(10)
    
    # # consumer_task.cancel()


# python -m celery -A src.common.queue.queue.app worker --loglevel=info --pool=solo
# python -m src.app-consumer
if __name__ == "__main__":
    send_task("src.worker.merge_file.merge_csv.merge_csv_task", "merge-csv", args=["file.csv"])



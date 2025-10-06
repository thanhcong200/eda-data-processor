from aiokafka import ConsumerRecord
from src.common.event_source.event_source_topic import EventData
from loguru import logger
from src.consumer.interface import MessageHandler

class UserHandler(MessageHandler):
    
    @staticmethod
    def get_name() -> str:
        return "UserHandler"
    
    @staticmethod
    def match(topic: str, message: ConsumerRecord) -> bool:
        if topic != UserHandler.get_topic():
            return False

        headers = {}
        if message.headers:
            headers = {
                k: v.decode("utf-8") if isinstance(v, bytes) else v
                for k, v in message.headers
            }

        event = headers.get("event")
        return event in UserHandler.get_events() if event else False
    
    @staticmethod
    def get_topic() -> str:
        return "user"
    
    @staticmethod
    def get_events() -> list[str]:
        return ["register", "login"]
    
    @staticmethod
    async def process( topic: str, partition: int, message: ConsumerRecord, parsed_message: EventData | None = None) -> None:
        logger.debug(f"[UserHandler] Receive message: {message}")
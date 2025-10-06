from abc import ABC, abstractmethod
from aiokafka import ConsumerRecord
from typing import Optional

from src.common.event_source.event_source_topic import EventData

class MessageHandler(ABC):
    @staticmethod
    @abstractmethod
    def match(topic: str, message: ConsumerRecord) -> bool:
        pass
    
    @staticmethod
    @abstractmethod
    def get_topic() -> str:
        pass
    
    @staticmethod
    @abstractmethod
    def get_events() -> list[str]:
        pass
    
    @staticmethod
    @abstractmethod
    def get_name() -> str:
        pass
    
    @staticmethod
    @abstractmethod
    async def process(topic: str, partition: int, message: ConsumerRecord, parsed_message: Optional[EventData] = None) -> None:
        pass
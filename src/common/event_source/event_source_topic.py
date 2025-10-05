from enum import Enum 
from typing import Dict, Any, Optional
from uuid import uuid4
from datetime import datetime, UTC
from dataclasses import dataclass
import json

from aiokafka import ConsumerRecord


class UserType(Enum):
    USER = 'user',
    ADMIN = 'admin'

class EventObjectType(Enum):
    USER = 'user'
    

class IEventSubject:
    type: UserType
    id: str 
    data: Optional[Any] = None

class IEventObject:
    type: EventObjectType
    id: str
    data: Optional[Any] = None

class IEventContext:
    req: Optional[Any] = None 
    
class IEventDataResponse:
    id: str
    topic: str
    event: str
    key: Optional[str] = None
    subject: Optional[IEventSubject] = None
    di_obj: Optional[IEventObject] = None 
    in_obj: Optional[IEventObject] = None 
    pr_obj: Optional[IEventObject] = None
    context: Optional[IEventContext] = None
    sent_at: int
    
@dataclass
class IEventData:
    id: Optional[str]
    topic: str
    event: str
    key: Optional[str] = None
    subject: Optional[Any] = None
    di_obj: Optional[Any] = None
    in_obj: Optional[Any] = None
    pr_obj: Optional[Any] = None
    context: Optional[Any] = None
    sent_at: Optional[datetime] = None
    other_data: Optional[Any] = None


class EventData:
    def __init__(self, data: IEventData):
        # Nếu id không có, tạo ObjectId mới
        self.id = data.id if data.id else str(uuid4())
        self.event = data.event
        self.topic = data.topic
        self.key = data.key
        self.subject = data.subject
        self.di_obj = data.di_obj
        self.in_obj = data.in_obj
        self.pr_obj = data.pr_obj
        self.context = data.context
        self.sent_at = data.sent_at or datetime.utcnow()
        self.other_data = data.other_data or None

    @staticmethod
    def new_from(data: dict) -> "EventData":
        return EventData(
            IEventData(
                id=str(data.get("id")) if data.get("id") else None,
                event=data["event"],
                topic=data["topic"],
                key=data.get("key"),
                subject=data.get("subject"),
                di_obj=data.get("di_obj"),
                in_obj=data.get("in_obj"),
                pr_obj=data.get("pr_obj"),
                context=data.get("context"),
                sent_at=datetime.fromtimestamp(data["sent_at"] / 1000) if data.get("sent_at") else datetime.utcnow(),
            )
        )

    def transform(self) -> dict:
        return {
            "id": str(self.id),
            "event": self.event,
            "topic": self.topic,
            "key": self.key,
            "subject": self.subject,
            "di_obj": self.di_obj,
            "in_obj": self.in_obj,
            "pr_obj": self.pr_obj,
            "context": self.context,
            "sent_at": int(self.sent_at.timestamp() * 1000),  # ms giống JS valueOf()
        }
        
    @staticmethod
    def parse_event_data(record: ConsumerRecord) -> "EventData":
        if isinstance(record.value, (bytes, bytearray)):
            payload = json.loads(record.value.decode("utf-8"))
        else:
            payload = record.value or {}

        # ✅ Parse headers
        headers: dict[str, str] = {}
        if record.headers:
            headers = {
                k: (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v))
                for k, v in record.headers
                if v is not None
            }

        # ✅ Parse key
        key_str: str | None = None
        if record.key is not None:
            key_str = record.key.decode("utf-8") if isinstance(record.key, (bytes, bytearray)) else str(record.key)

        # ✅ Build EventData-compatible dict
        data = {
            **payload,
            "event": headers.get("event") or payload.get("event"),
            "id": headers.get("id") or payload.get("id"),
            "topic": record.topic,
            "key": key_str,
            "sent_at": payload.get("sent_at") or int(datetime.now(UTC).timestamp() * 1000),
        }

        return EventData.new_from(data)
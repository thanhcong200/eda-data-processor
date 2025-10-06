import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from celery import Celery
from kombu import Queue
from loguru import logger
from typing import Any

from src.config.environment import REDIS_URI, APP_NAME
# REDIS_URI="redis://localhost:6379"
# APP_NAME="data-processor"

app: Celery = Celery(
    f"woker::{APP_NAME}",
    broker=REDIS_URI,
    backend=REDIS_URI,
    include=[
        "src.worker.merge_file.merge_csv",
        "src.worker.merge_file.merge_excel"
    ]
)    

app.conf.task_queues = [
    Queue("merge-csv"),
    Queue("merge-excel"),
]    

app.conf.update(
    task_default_exchange="default",
    task_default_queue="default",
    task_default_routing_key="default",
    task_routes={
        "src.worker.merge_file.merge_csv.*": {"queue": "merge-csv"},
        "src.worker.merge_file.merge_excel.*": {"queue": "merge-excel"},
    },
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Ho_Chi_Minh",
    enable_utc=True,
)

def send_task(task_name: str, queue_name: str, args=None, kwargs = None) -> Any:
    logger.info(f"📤 Sending task '{task_name}' to queue '{queue_name}'...")
    result = app.send_task(task_name, args=args or None, kwargs=kwargs or None, queue=queue_name)
    return result
    
    
        
            
        
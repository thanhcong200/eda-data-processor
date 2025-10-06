from celery import shared_task
from loguru import logger
import time


@shared_task(name="src.worker.merge_file.merge_excel.merge_excel_task")
def merge_excel_task(file_id: str):
    logger.info(f"🧩 Start merging merge_excel_file: {file_id}")
    time.sleep(3)
    logger.info(f"✅ Merged merge_excel_file successfully: {file_id}")
    return {"status": "done", "file_id": file_id}

from loguru import logger
from datetime import datetime
import json
import sys

from config.environment import LOG_LEVEL, LOG_OUTPUT_JSON 

logger.remove()

if LOG_OUTPUT_JSON:
    # Log JSON gọn gàng
    def json_formatter(record):
        base = {
            "time": record["time"].strftime("%Y-%m-%d %H:%M:%S"),
            "level": record["level"].name,
            "message": record["message"],
        }
        if record["extra"]:
            base.update(record["extra"])
        return json.dumps(base)

    logger.add(sys.stdout, level=LOG_LEVEL.upper(), format=json_formatter, colorize=False)
else:
    # Log text có màu nhưng gọn
    def text_formatter(record):
        timestamp = record["time"].strftime("%Y-%m-%d %H:%M:%S")
        level = record["level"].name
        message = record["message"]
        extras = (
            json.dumps(record["extra"], ensure_ascii=False)
            if record["extra"]
            else ""
        )
        return f"<green>{timestamp}</green> | <level>{level}</level> | {message} {extras}"

    logger.add(sys.stdout, level=LOG_LEVEL.upper(), format=text_formatter, colorize=True)
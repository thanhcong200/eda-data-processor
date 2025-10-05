import os
from dotenv import load_dotenv

load_dotenv()

# app
NODE_ENV:str = os.getenv('NODE_ENV', 'development')
APP_NAME:str = os.getenv('APP_NAME', 'data-processor')
LOG_LEVEL:str = os.getenv('LOG_LEVEL', 'debug')
LOG_OUTPUT_JSON: int = int(os.getenv('LOG_OUTPUT_JSON', 1))
PORT:int = int(os.getenv('PORT', 8000))

# database
DATABASE_CLIENT:str = os.getenv('DATABASE_CLIENT', 'postgres')
DATABASE_HOST:str = os.getenv('DATABASE_HOST', '')
DATABASE_PORT:int = int(os.getenv('DATABASE_PORT', 5432))
DATABASE_USER:str = os.getenv('DATABASE_USER', '')
DATABASE_PASSWORD:str = os.getenv('DATABASE_PASSWORD', '')

# redis
REDIS_URI:str = os.getenv('REDIS_URI', '')

# kafka
KAFKA_BROKERS:str = os.getenv('KAFKA_BROKERS', '')
KAFKA_USER:str = os.getenv('KAFKA_USER', '')
KAFKA_PASSWORD: str = os.getenv('KAFKA_PASSWORD', '')


import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

logger.add("server.log", rotation="10 MB", compression="zip", level="INFO")

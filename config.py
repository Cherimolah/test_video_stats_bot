import os

from dotenv import load_dotenv


load_dotenv()


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENROUTER_TOKEN = os.getenv("OPENROUTER_TOKEN")

DATABASE_URL = os.getenv("DATABASE_URL")

MODEL_NAME = os.getenv("MODEL_NAME")

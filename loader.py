from aiogram import Bot, Dispatcher
from openai import OpenAI

from config import TELEGRAM_TOKEN, OPENROUTER_TOKEN


bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

client = OpenAI(
  base_url="https://openrouter.ai/api/v1",
  api_key=OPENROUTER_TOKEN,
)

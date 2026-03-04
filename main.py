import asyncio

from loguru import logger

from loader import bot, dp
import handlers
import logs
from db_engine import get_count_videos, create_tables, load_data


async def main():
    await create_tables()
    count = await get_count_videos()
    if count == 0:
        logger.warning('No videos found. Data will automatically loaded from "videos.json"')
        await load_data()
        logger.success('Data successfully loaded')
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())

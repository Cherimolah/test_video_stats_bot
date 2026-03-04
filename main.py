import asyncio

from loguru import logger

from loader import bot, dp
import handlers
import logs
from db_engine import get_count_videos, create_tables, load_data


async def main():
    """
    Основная функция
    :return:
    """
    # Создаем таблички
    await create_tables()
    count = await get_count_videos()
    # Если записей о видео нет, загружаем их из файла videos.txt
    if count == 0:
        logger.warning('No videos found. Data will automatically loaded from "videos.json"')
        await load_data()
        logger.success('Data successfully loaded')
    # Запускаем поллинг бота
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())

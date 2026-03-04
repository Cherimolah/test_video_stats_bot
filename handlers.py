from aiogram.types import Message
from aiogram.filters.command import CommandStart
from aiogram.enums.parse_mode import ParseMode
from aiogram import F

from loader import dp
from utils import prompt, request_llm, extract_sql, check_sql
from db_engine import run_raw_sql


@dp.message(CommandStart())
async def start(message: Message):
    """
    Хендлер для обработки привествтенного сообщения
    :param message: объект aiogram
    :return:
    """
    await message.answer('Привет! 👋 Я бот со встроенным искусственным интеллектом, '
                         'который помогает получать статистику видео разных креаторов.\n\n'
                         '*✨ Что я могу:*\n'
                         '— Отвечать на вопросы *на естественном языке*. Просто спроси:\n'
                         '_«Сколько всего видео есть в системе?»_\n'
                         '_«Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?»_\n'
                         '_«Сколько видео набрало больше 100 000 просмотров за всё время?»_\n'
                         '— Доставать данные из базы: просмотры, лайки, комментарии, даты публикации и другие метрики.\n'
                         '— Давать аналитику по отдельным видео или по всем видео креатора.\n'
                         '— Отвечать на вопросы о трендах, сравнениях, лучших видео и т.п.\n\n'
                         '*💬 Как пользоваться:*\n'
                         'Просто напиши свой запрос как другу. Я пойму, что ты имеешь в виду, '
                         'обращусь к базе данных и дам точный ответ.\n\n'
                         'Например:\n'
                         '— «На сколько просмотров в сумме выросли все видео 28 ноября 2025?»\n'
                         '— «Сколько разных видео получали новые просмотры 27 ноября 2025?»\n\n'
                         'Попробуй прямо сейчас! 🚀 Задай любой вопрос о статистике видео.',
                         parse_mode=ParseMode.MARKDOWN)


@dp.message(F.text)
async def request_user(message: Message):
    """
    Хендлер для обработки запроса пользователя к базе данных на естественном языке
    :param message: объект aiogram
    :return:
    """
    # Форматируем в заранее подготвленный промпт к LLM
    text = prompt.format(message.text)
    # Получаем в ответ SQL од, форматированный в блок кода разметкой Markdown
    response = await request_llm(text)
    # Вытаскиваем из разметки сам текст кода
    sql = extract_sql(response)
    # Проверяем, что SQL запрос действительно существует
    if not sql:
        await message.answer('К сожалению, нам не удалось сгенерировать SQL запрос для получения данных')
        return
    # Дополнительная проверка на то, что SQL запрос безопасный и его использовать
    # Метод не даёт 100% гарантий, т.к. всегда есть вероятность галлюцинации LLM, но большинство угроз он предотвращает
    safely = await check_sql(sql)
    if not safely:
        await message.answer('К сожалению, нам не удалось сгенерировать SQL запрос для получения данных')
        return
    # Если все ок, выполняем SQL запрос и возвращаем ответ пользователю
    response = await run_raw_sql(sql)
    await message.answer(f'{response}')

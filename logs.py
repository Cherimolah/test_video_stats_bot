import logging
import sys

from loguru import logger

# Удаляем стандартный обработчик loguru, который по умолчанию выводит логи в stderr.
# Это делается для того, чтобы настроить свой вывод с нужным форматом и уровнем.
logger.remove()

# Добавляем новый обработчик, который будет выводить логи в стандартный поток вывода (sys.stdout).
# Уровень логирования – INFO (сообщения уровня DEBUG и ниже игнорируются).
# Задаём пользовательский формат сообщения:
#   - время в зелёном цвете,
#   - уровень логирования,
#   - имя модуля, функции и номер строки,
#   - само сообщение.
logger.add(
    sys.stdout,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
           "<level>{message}</level>"
)


# Создаём собственный обработчик для стандартного модуля logging.
# Этот обработчик будет перенаправлять все записи из logging в loguru.
class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Пытаемся получить уровень логирования из loguru по имени уровня (например, "INFO").
        # Если такого уровня нет в loguru, используем числовое значение уровня из logging.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Отправляем сообщение в loguru.
        # opt(depth=6) – корректировка глубины стека для правильного отображения места вызова,
        # exception=record.exc_info – передаём информацию об исключении, если оно есть.
        logger.opt(depth=6, exception=record.exc_info).log(
            level, record.getMessage()
        )


# Настраиваем корневой логгер модуля logging так, чтобы он использовал наш перехватчик.
# level=0 означает, что будут обрабатываться все уровни логирования.
# force=True принудительно удаляет все предыдущие настройки корневого логгера.
logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

# Для библиотек aiogram и aiohttp явно устанавливаем обработчик и запрещаем передачу записей
# в родительские логгеры (propagate=False), чтобы избежать дублирования сообщений.
for name in ("aiogram", "aiohttp"):
    logging.getLogger(name).handlers = [InterceptHandler()]
    logging.getLogger(name).propagate = False

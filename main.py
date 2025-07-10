# Файл: main.py
import sys
import argparse

from logger import logger
from pipeline import run_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Поиск дубликатов изображений. По умолчанию работает в режиме Redis.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Флаг для включения локального режима. По умолчанию выключен (т.е. use_redis=True).
    parser.add_argument(
        '--local',
        action='store_false',
        dest='use_redis',  # Результат сохранится в parsed_args.use_redis
        help="Отключить использование Redis и работать с локальными директориями."
    )
    parser.set_defaults(use_redis=True)
    
    parsed_args = parser.parse_args()

    # Проверяем CHAT_ID только если мы действительно работаем в режиме Redis
    if parsed_args.use_redis:
        # Импортируем CHAT_ID здесь, локально, чтобы избежать ошибок при --local
        from redis_handler import CHAT_ID
        if not CHAT_ID:
            logger.error("Ошибка: для работы в режиме Redis необходимо установить переменную окружения CHAT_ID.")
            sys.exit(1)
    
    # Запускаем основной конвейер обработки
    try:
        run_pipeline(parsed_args.use_redis)
    except Exception as e:
        logger.critical("В процессе выполнения конвейера произошла непредвиденная ошибка: %s", e, exc_info=True)
        sys.exit(1)
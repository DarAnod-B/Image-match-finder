import os
import sys
import argparse

from logger import logger
from redis_utils import CHAT_ID
from pipeline import run_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Поиск дубликатов изображений. По умолчанию работает в режиме Redis.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Мы используем action='store_false', что означает:
    # - Если флаг --local присутствует, destination ('use_redis') будет False.
    # - Если флаг отсутствует, будет использовано значение по умолчанию (True).
    parser.add_argument(
        '--local',
        action='store_false',
        dest='use_redis',  # Сохраняем имя переменной как 'use_redis' для совместимости
        help="Отключить использование Redis и работать с локальными директориями.\n"
             "По умолчанию, скрипт использует Redis."
    )
    # Явно устанавливаем, что по умолчанию use_redis должно быть True.
    parser.set_defaults(use_redis=True)
    
    parsed_args = parser.parse_args()

    # Она будет срабатывать, когда use_redis = True (т.е. по умолчанию).
    if parsed_args.use_redis and not CHAT_ID:
        logger.error("Ошибка: для работы в режиме Redis необходимо установить переменную окружения CHAT_ID.")
        sys.exit(1)
    
    # Запускаем основной конвейер обработки.
    # Внутренние модули не меняются, так как они просто получают bool-значение.
    try:
        run_pipeline(parsed_args.use_redis)
    except Exception as e:
        logger.critical("В процессе выполнения конвейера произошла непредвиденная ошибка: %s", e, exc_info=True)
        sys.exit(1)

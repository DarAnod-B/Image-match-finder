"""
Модуль для взаимодействия с Redis.

Отвечает за:
- Подключение к серверу Redis.
- Получение CHAT_ID из переменных окружения.
- Предоставление функций для извлечения данных для Group 1 (из CSV) и Group 2 (из списка).
"""
import os
import sys
import redis
import csv
import io
from typing import List

# Импортируем общие компоненты проекта
from logger import logger
from settings import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    CSV_IMAGE_COLUMN, CSV_CELL_DELIMITER, CSV_IMAGE_DELIMITER
)
from core.utils import validate_image_file

# --- ИНИЦИАЛИЗАЦИЯ И ПРОВЕРКА ЗАВИСИМОСТЕЙ ---

try:
    # CHAT_ID является обязательной зависимостью для этого модуля
    CHAT_ID = os.environ['CHAT_ID']
    logger.info("Модуль redis_handler использует CHAT_ID: %s", CHAT_ID)
except KeyError:
    logger.error("Критическая ошибка: переменная окружения CHAT_ID не установлена!")
    sys.exit(1)

# Определяем ключи для Redis на основе CHAT_ID
_REDIS_CSV_KEY = f'{CHAT_ID}:csv:raw'
_REDIS_GROUP2_KEY = f'{CHAT_ID}:group2_images'

# --- ПОДКЛЮЧЕНИЕ К REDIS ---

try:
    # Создаем клиент Redis, который будет использоваться функциями этого модуля
    _redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True  # Автоматически декодировать ответы из utf-8
    )
    # Проверяем соединение
    _redis_client.ping()
    logger.info("Успешное подключение к Redis по адресу %s:%d", REDIS_HOST, REDIS_PORT)
except redis.exceptions.ConnectionError as e:
    logger.error("Не удалось подключиться к Redis: %s", e)
    sys.exit(1)


def get_group1_image_paths() -> List[str]:
    """
    Извлекает CSV из Redis, парсит его и возвращает список путей к изображениям для Group 1.
    """
    logger.info("Получение CSV для Group 1 из Redis (ключ: '%s')", _REDIS_CSV_KEY)

    csv_data = _redis_client.get(_REDIS_CSV_KEY)
    if not csv_data:
        logger.error("Данные CSV не найдены в Redis по ключу '%s'", _REDIS_CSV_KEY)
        return []

    image_paths = []
    csv_file = io.StringIO(csv_data)
    reader = csv.reader(csv_file, delimiter=CSV_CELL_DELIMITER)

    try:
        headers = next(reader)
        if CSV_IMAGE_COLUMN not in headers:
            logger.error("В CSV отсутствует обязательная колонка: '%s'", CSV_IMAGE_COLUMN)
            return []
        
        image_col_index = headers.index(CSV_IMAGE_COLUMN)

        for row in reader:
            if len(row) > image_col_index and row[image_col_index]:
                paths_in_cell = row[image_col_index].split(CSV_IMAGE_DELIMITER)
                image_paths.extend([path.strip() for path in paths_in_cell if path.strip()])
                
    except StopIteration:
        logger.warning("CSV файл в Redis пуст (содержит только заголовки или ничего).")
    except Exception as e:
        logger.error("Ошибка при парсинге CSV из Redis: %s", e)
        return []

    logger.info("Извлечено %d путей изображений из CSV для Group 1.", len(image_paths))
    
    valid_paths = [path for path in image_paths if validate_image_file(path)]
    if len(valid_paths) != len(image_paths):
        logger.warning("Найдено %d валидных файлов изображений из %d.", len(valid_paths), len(image_paths))

    return valid_paths


def get_group2_image_paths() -> List[str]:
    """
    Получает список путей к изображениям для Group 2 из списка Redis.
    """
    logger.info("Получение списка изображений для Group 2 из Redis (ключ: '%s')", _REDIS_GROUP2_KEY)
    
    if not _redis_client.exists(_REDIS_GROUP2_KEY):
        logger.error("Ключ '%s' для Group 2 не найден в Redis.", _REDIS_GROUP2_KEY)
        return []

    # lrange(key, 0, -1) получает все элементы списка
    image_paths = _redis_client.lrange(_REDIS_GROUP2_KEY, 0, -1)
    
    logger.info("Получено %d путей для Group 2 из Redis.", len(image_paths))

    valid_paths = [path for path in image_paths if validate_image_file(path)]
    if len(valid_paths) != len(image_paths):
        logger.warning("Найдено %d валидных файлов изображений из %d.", len(valid_paths), len(image_paths))

    return valid_paths
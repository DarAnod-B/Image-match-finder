"""
Модуль для взаимодействия с Redis.

Реализован по "ленивому" принципу: подключение к серверу и проверка
переменных окружения происходят только при первой необходимости, а не
при импорте модуля. Это делает его безопасным для использования в
любом режиме работы приложения.
"""
import os
import redis
import csv
import io
from typing import List, Optional

# Импортируем общие компоненты проекта
from logger import logger
from settings import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    CSV_IMAGE_COLUMN, CSV_CELL_DELIMITER, CSV_IMAGE_DELIMITER
)
from core.utils import validate_image_file

# --- БЕЗОПАСНАЯ ИНИЦИАЛИЗАЦИЯ ПЕРЕМЕННЫХ ---

# Просто читаем переменную. Будет None, если не установлена. Не вызывает ошибок.
CHAT_ID: Optional[str] = os.getenv('CHAT_ID')

# Приватная переменная для хранения клиента. Будет создана лениво.
_redis_client: Optional[redis.Redis] = None


def get_redis_client() -> Optional[redis.Redis]:
    """
    "Ленивый" и безопасный способ получить клиент Redis.
    Подключается только при первом вызове. Возвращает None в случае ошибки.
    """
    global _redis_client

    # Если клиент уже успешно создан, просто возвращаем его
    if _redis_client:
        return _redis_client

    # Первый вызов: выполняем все проверки и подключение
    if not CHAT_ID:
        logger.error("Критическая ошибка: попытка использовать Redis, но CHAT_ID не установлен!")
        return None

    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True  # Автоматически декодировать ответы из utf-8
        )
        # Проверяем соединение
        client.ping()
        logger.info("Успешное подключение к Redis по адресу %s:%d", REDIS_HOST, REDIS_PORT)
        
        # Сохраняем успешный клиент в глобальную переменную для последующих вызовов
        _redis_client = client
        return _redis_client

    except redis.exceptions.ConnectionError as e:
        logger.error("Не удалось подключиться к Redis: %s", e)
        return None


def get_group1_image_paths_with_indices() -> List[tuple[int, str]]:
    """
    Извлекает CSV из Redis, парсит его и возвращает список кортежей
    (индекс, путь) для изображений Group 1, сохраняя их исходный порядок.
    """
    client = get_redis_client()
    if not client:
        return []  # Не удалось получить клиент, возвращаем пустой список

    _REDIS_CSV_KEY = f'{CHAT_ID}:csv:raw'
    logger.info("Получение CSV для Group 1 из Redis (ключ: '%s')", _REDIS_CSV_KEY)

    csv_data = client.get(_REDIS_CSV_KEY)
    if not csv_data:
        logger.error("Данные CSV не найдены в Redis по ключу '%s'", _REDIS_CSV_KEY)
        return []

    # Этот список будет хранить кортежи (индекс, путь)
    indexed_paths = []
    # Используем счетчик для создания уникального последовательного индекса
    current_index = 0
    
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
                # Разделяем пути внутри одной ячейки
                paths_in_cell = row[image_col_index].split(CSV_IMAGE_DELIMITER)
                
                for path in paths_in_cell:
                    clean_path = path.strip()
                    # Проверяем, что путь не пустой и файл валидный
                    if clean_path and validate_image_file(clean_path):
                        # КЛЮЧЕВОЙ МОМЕНТ: добавляем кортеж с текущим индексом и путем
                        indexed_paths.append((current_index, clean_path))
                        # Увеличиваем индекс для следующего изображения
                        current_index += 1
                    elif clean_path:
                        logger.warning("Пропущен невалидный путь к файлу: %s", clean_path)

    except StopIteration:
        logger.warning("CSV файл в Redis пуст (содержит только заголовки или ничего).")
    except Exception as e:
        logger.error("Ошибка при парсинге CSV из Redis: %s", e)
        return []

    logger.info("Извлечено и проиндексировано %d валидных путей изображений из CSV для Group 1.", len(indexed_paths))
    
    return indexed_paths


def get_group2_dir_paths() -> str|None:
    """
    Получает ПУТЬ к директории из Redis, а затем сканирует ее
    с помощью общей функции `collect_from_dir`.
    """
    client = get_redis_client()
    if not client:
        return None

    # Предполагаем, что в Redis по этому ключу лежит ОДИН путь к директории
    _REDIS_GROUP2_KEY = f'{CHAT_ID}:GROUP2_DIR_IMAGES' 
    logger.info("Получение пути к директории для Group 2 из Redis (ключ: '%s')", _REDIS_GROUP2_KEY)
    
    directory_path = client.get(_REDIS_GROUP2_KEY)

    if not directory_path:
        logger.error("Путь к директории не найден в Redis по ключу '%s'", _REDIS_GROUP2_KEY)
        return None

    logger.info("Путь из Redis получен: '%s'. Запускаем сканирование...", directory_path)
    
    return directory_path.strip() 
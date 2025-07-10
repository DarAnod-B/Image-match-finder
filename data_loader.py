import os
from typing import List

from logger import logger
from settings import GROUP_1_DIR, GROUP_2_DIR
from core.utils import validate_image_file

def _collect_from_local_dir(directory: str) -> List[str]:
    """
    Приватная функция для сбора и валидации путей из локальной директории.
    """
    paths = []
    if not os.path.isdir(directory):
        logger.error("Директория не найдена: %s", directory)
        return paths
        
    logger.info("Сканирование и валидация изображений в: %s", directory)
    for filename in sorted(os.listdir(directory)):
        full_path = os.path.join(directory, filename)
        if validate_image_file(full_path):
            paths.append(full_path)
            
    logger.info("Найдено %d валидных изображений.", len(paths))
    return paths

def get_group1_image_paths(use_redis_mode: bool) -> List[str]:
    """
    Возвращает список путей к изображениям для Group 1, 
    выбирая источник на основе режима работы.
    """
    if use_redis_mode:
        logger.info("Режим: Redis. Получение данных для Group 1...")
        from redis_handler import get_group1_image_paths as get_from_redis
        return get_from_redis()
    else:
        logger.info("Режим: Локальный. Сканирование директории '%s'...", GROUP_1_DIR)
        return _collect_from_local_dir(GROUP_1_DIR)

def get_group2_image_paths(use_redis_mode: bool) -> List[str]:
    """
    Возвращает список путей к изображениям для Group 2, 
    выбирая источник на основе режима работы.
    """
    if use_redis_mode:
        logger.info("Режим: Redis. Получение данных для Group 2...")
        from redis_handler import get_group2_image_paths as get_from_redis
        return get_from_redis()
    else:
        logger.info("Режим: Локальный. Сканирование директории '%s'...", GROUP_2_DIR)
        return _collect_from_local_dir(GROUP_2_DIR)
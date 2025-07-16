import os
from typing import List

from logger import logger
from settings import GROUP_1_DIR, GROUP_2_DIR
from core.utils import validate_image_file


def collect_from_dir(directory: str) -> List[str]:
    """
    Приватная функция для сбора и валидации путей из директории.
    """
    paths = []
    if not os.path.isdir(directory):
        logger.error("Директория не найдена: %s", directory)
        return paths
        
    logger.info("Сканирование и валидация изображений в: %s", directory)
    for filename in os.listdir(directory):
        full_path = os.path.join(directory, filename)
        if validate_image_file(full_path):
            paths.append(full_path)
            
    logger.info("Найдено %d валидных изображений.", len(paths))
    return paths

def collect_from_dir_with_indices(directory: str):
    """
    Собирает, валидирует пути из директории и присваивает им индекс.
    Сортировка по имени файла обеспечивает предсказуемый и стабильный порядок.
    """
    paths = []
    if not os.path.isdir(directory):
        logger.error("Директория не найдена: %s", directory)
        return []
        
    logger.info("Сканирование и валидация изображений в: %s", directory)
    
    # Сортируем файлы по имени для стабильного порядка перед индексацией
    try:
        sorted_filenames = sorted(os.listdir(directory))
    except FileNotFoundError:
        logger.error("Не удалось прочитать директорию (возможно, удалена во время работы): %s", directory)
        return []
        
    for filename in sorted_filenames:
        full_path = os.path.join(directory, filename)
        if validate_image_file(full_path):
            paths.append(full_path)
    
    # КЛЮЧЕВОЙ МОМЕНТ: Используем enumerate для создания кортежей (индекс, путь)
    indexed_paths = list(enumerate(paths))
    
    logger.info("Найдено и проиндексировано %d валидных изображений.", len(indexed_paths))
    return indexed_paths


def get_group1_image_paths_with_indices(use_redis_mode: bool):
    """
    Возвращает список кортежей (индекс, путь) для изображений Group 1,
    выбирая источник на основе режима работы.
    """
    if use_redis_mode:
        logger.info("Режим: Redis. Получение проиндексированных данных для Group 1...")
        # Вызываем нашу новую функцию из redis_handler
        from redis_handler import get_group1_image_paths_with_indices as get_from_redis
        return get_from_redis()
    else:
        logger.info("Режим: Локальный. Сканирование и индексация директории '%s'...", GROUP_1_DIR)
        # Вызываем новую локальную функцию с индексацией
        return collect_from_dir_with_indices(GROUP_1_DIR)

def get_group2_image_paths(use_redis_mode: bool) -> List[str]:
    """
    Возвращает список путей к изображениям для Group 2.
    Здесь индексация не нужна, так как это база для поиска.
    """
    if use_redis_mode:
        logger.info("Режим: Redis. Получение данных для Group 2...")
        from redis_handler import get_group2_dir_paths 
        dir_path = get_group2_dir_paths()
        
        # Здесь используется обычная функция сбора без индексов
        return collect_from_dir(dir_path) if dir_path else []
    else:
        logger.info("Режим: Локальный. Сканирование директории '%s'...", GROUP_2_DIR)
        return collect_from_dir(GROUP_2_DIR)
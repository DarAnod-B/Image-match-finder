# main.py
"""
Главный исполняемый файл для поиска дубликатов изображений.

Этот скрипт оркестрирует весь процесс, который разбит на три этапа:
1. `build_descriptor_cache`: Принудительно создает кэш дескрипторов для
   базовой группы изображений (Group 2).
2. `process_query_images`: Использует созданный кэш для поиска аналогов
   для каждой картинки из целевой группы (Group 1).
3. `report_results`: Аккуратно форматирует и выводит итоговый список в консоль.

Все пути и технические параметры настраиваются в файле `settings.py`.
"""
import os
from typing import List

# Импорты адаптированы под вашу структуру
from logger import logger
from settings import (
    GROUP_1_DIR, GROUP_2_DIR, DESCRIPTORS_CACHE_PATH
)
from core.cacher import DescriptorCacher
from core.searcher import Searcher
from core.utils import validate_image_file


def collect_valid_images(directory: str) -> List[str]:
    """
    Собирает и валидирует пути к изображениям из указанной директории.
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


def build_descriptor_cache() -> bool:
    """
    Создает кэш дескрипторов для изображений из GROUP_2_DIR.
    
    Returns:
        bool: True в случае успеха, False в случае ошибки.
    """
    logger.info("--- ЭТАП 1: Создание кэша дескрипторов ---")
    
    group2_paths = collect_valid_images(GROUP_2_DIR)
    if not group2_paths:
        logger.error("В директории '%s' нет изображений для создания кэша. Завершение работы.", GROUP_2_DIR)
        return False
        
    cacher = DescriptorCacher(group2_paths)
    cacher.create_and_save_cache(DESCRIPTORS_CACHE_PATH)
    return True


def process_query_images() -> List[str]:
    """
    Использует кэш для обработки изображений из GROUP_1_DIR и формирует финальный список изображний.
    Если не удалось найти чистый аналог, то добавляет исходную версию.

    Returns:
        List[str]: Итоговый "очищенный" список путей к изображениям.
    """
    logger.info("--- ЭТАП 2: Поиск аналогов для изображений из Group 1 ---")
    final_list = []
    
    try:
        searcher = Searcher(DESCRIPTORS_CACHE_PATH)
    except Exception as e:
        logger.error("Критическая ошибка: не удалось загрузить кэш '%s': %s", DESCRIPTORS_CACHE_PATH, e)
        return final_list # Возвращаем пустой список в случае ошибки
        
    group1_paths = collect_valid_images(GROUP_1_DIR)
    
    for img1_path in group1_paths:
        logger.info("=" * 60)
        logger.info("Поиск для: %s", os.path.basename(img1_path))
        
        found_match_path = searcher.find_match(img1_path)
        
        if found_match_path:
            logger.info("✅ Найден чистый аналог: %s", found_match_path)
            final_list.append(found_match_path)
        else:
            logger.warning("⚠️ Чистый аналог не найден. Добавляем исходную версию: %s", img1_path)
            final_list.append(img1_path)
            
    return final_list


def report_results(final_image_list: List[str]):
    """
    Форматирует и выводит итоговый результат в консоль.
    
    Args:
        final_image_list: Финальный список путей к изображениям.
    """
    logger.info("\n" + "="*60)
    logger.info("ИТОГ: Сформирован финальный список из %d изображений:", len(final_image_list))
    
    if final_image_list:
        for img_path in final_image_list:
            print(f"🖼️  {img_path}")
    else:
        logger.info("Финальный список пуст.")
    
    logger.info("="*60)


def main():
    """
    Главная функция-оркестратор.
    Последовательно запускает этапы создания кэша, поиска и вывода результатов.
    """
    # 1. Создать кэш. Если не удалось, прекратить работу.
    if not build_descriptor_cache():
        return

    # 2. Обработать изображения и получить финальный список изображений.
    final_list = process_query_images()

    # 3. Вывести отчет.
    report_results(final_list)


if __name__ == "__main__":
    main()

import os
from typing import List

from logger import logger
from settings import DESCRIPTORS_CACHE_PATH
from core.cacher import DescriptorCacher
from core.searcher import Searcher

# Модули для работы с данными
import data_loader 
from image_link_manager import ImageLinkManager
from redis_utils import redis_client, CHAT_ID

def _build_descriptor_cache(use_redis_mode: bool) -> bool:
    """
    ЭТАП 1: Создает кэш дескрипторов для изображений из Group 2.
    """
    logger.info("--- ЭТАП 1: Создание кэша дескрипторов ---")
    
    group2_paths = data_loader.get_group2_image_paths(use_redis_mode)

    if not group2_paths:
        logger.error("В источнике данных нет изображений для Group 2. Завершение работы.")
        return False
        
    cacher = DescriptorCacher(group2_paths)
    cacher.create_and_save_cache(DESCRIPTORS_CACHE_PATH)
    return True

def _process_query_images(use_redis_mode: bool) -> List[str]:
    """
    ЭТАП 2: Использует кэш для обработки изображений из Group 1.
    """
    logger.info("--- ЭТАП 2: Поиск аналогов для изображений из Group 1 ---")
    final_list = []
    
    try:
        searcher = Searcher(DESCRIPTORS_CACHE_PATH)
    except Exception as e:
        logger.error("Критическая ошибка: не удалось загрузить кэш '%s': %s", DESCRIPTORS_CACHE_PATH, e)
        return final_list

    group1_paths = data_loader.get_group1_image_paths(use_redis_mode)
        
    if not group1_paths:
        logger.warning("В источнике данных для Group 1 не найдено изображений. Пропускаем этап поиска.")
        return final_list

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

def _update_source_and_report(final_image_list: List[str], use_redis_mode: bool):
    """
    ЭТАП 3: Обновляет источник данных (если Redis) и выводит отчет.
    """
    logger.info("\n--- ЭТАП 3: Обновление источника и формирование отчета ---")

    if use_redis_mode:
        logger.info("Режим: Redis. Обновление CSV в Redis...")
        manager = ImageLinkManager.from_redis(redis_client, CHAT_ID)

        if not manager:
            logger.error("Не удалось инициализировать менеджер CSV из Redis. Обновление отменено.")
        elif manager.rows:
            manager.set_image_links(0, final_image_list)
            updated_content = manager.save_changes_and_get_content()
            if updated_content:
                redis_client.set(f'{CHAT_ID}:csv:raw', updated_content)
                logger.info("✅ Обновлённый CSV успешно сохранён в Redis.")
            else:
                logger.error("Ошибка при сохранении CSV файла на диск.")
        else:
            logger.warning("В CSV из Redis не найдено строк для обновления.")

    logger.info("\n" + "="*60)
    logger.info("ИТОГ: Сформирован финальный список из %d изображений:", len(final_image_list))
    
    if final_image_list:
        for img_path in final_image_list:
            print(f"🖼️  {img_path}")
    else:
        logger.info("Финальный список пуст.")
    
    logger.info("="*60)

def run_pipeline(use_redis: bool):
    """
    Главная функция-оркестратор. Запускает все этапы конвейера.
    """
    # Этап 1: Создать кэш
    if not _build_descriptor_cache(use_redis):
        return

    # Этап 2: Обработать изображения и получить финальный список путей
    final_list = _process_query_images(use_redis)

    # Этап 3: Обновить источник (если нужно) и вывести отчет
    _update_source_and_report(final_list, use_redis)

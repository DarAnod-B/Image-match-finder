"""
Модуль-оркестратор, определяющий основной конвейер обработки.

Отвечает за:
- Загрузку конфигураций в зависимости от режима работы.
- Последовательный вызов этапов:
  1. Создание кэша дескрипторов.
  2. Поиск аналогов для изображений.
  3. Обновление источника данных (Redis) и/или вывод отчета.
"""
import os
from typing import List

from logger import logger
from settings import DESCRIPTORS_CACHE_PATH
from core.cacher import DescriptorCacher
from core.searcher import Searcher

# Модули для работы с данными
import data_loader
from image_link_manager import ImageLinkManager

# Redis
from redis_handler import get_redis_client, CHAT_ID

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

def _process_query_images(use_redis_mode: bool, keep_unmatched_images: bool) -> List[str]:
    """
    ЭТАП 2: Использует кэш для обработки изображений из Group 1.
    Сохраняет исходный порядок изображений.
    """
    logger.info("--- ЭТАП 2: Поиск аналогов для изображений из Group 1 ---")
    final_list = []

    try:
        searcher = Searcher(DESCRIPTORS_CACHE_PATH)
    except Exception as e:
        logger.error("Критическая ошибка: не удалось загрузить кэш '%s': %s", DESCRIPTORS_CACHE_PATH, e)
        return final_list

    group1_paths = data_loader.get_group1_image_paths(use_redis_mode)
    logger.debug("G1 after loader  : %s", group1_paths)

    if not group1_paths:
        logger.warning("В источнике данных для Group 1 не найдено изображений.")
        return final_list

    # Цикл по изображениям сохраняет исходный порядок
    for img1_path in group1_paths:
        logger.info("=" * 60)
        logger.info("Поиск для: %s", os.path.basename(img1_path))

        found_match_path = searcher.find_match(img1_path)

        if found_match_path:
            logger.info("✅ Найден чистый аналог: %s", found_match_path)
            final_list.append(found_match_path)
        else:
            logger.warning("⚠️ Чистый аналог не найден для: %s", os.path.basename(img1_path))

            # Логика добавления исходного изображения теперь условная
            if keep_unmatched_images:
                logger.info("Сохраняем исходное изображение согласно настройкам (KEEP_UNMATCHED_IMAGES=True).")
                final_list.append(img1_path)
            else:
                logger.info("Отбрасываем исходное изображение согласно настройкам (KEEP_UNMATCHED_IMAGES=False).")
    logger.debug("G1 final_list    : %s", final_list)

    return final_list

def _update_source_and_report(final_image_list: List[str], use_redis_mode: bool):
    """
    ЭТАП 3: Обновляет источник данных (если Redis) и выводит отчет.
    """
    logger.info("\n--- ЭТАП 3: Обновление источника и формирование отчета ---")

    if use_redis_mode:
        logger.info("Режим: Redis. Обновление CSV в Redis...")

        redis_client = get_redis_client()

        if redis_client and CHAT_ID:
            manager = ImageLinkManager.from_redis(redis_client, CHAT_ID)

            if not manager:
                logger.error("Не удалось инициализировать менеджер CSV из Redis. Обновление отменено.")
            elif manager.rows:
                # Обновляем первую (и единственную) строку
                manager.set_image_links(0, final_image_list)
                updated_content = manager.save_changes_and_get_content()

                if updated_content:
                    redis_client.set(f'{CHAT_ID}:csv:raw', updated_content)
                    logger.info("✅ Обновлённый CSV успешно сохранён в Redis.")
                else:
                    logger.error("Ошибка при сохранении CSV файла на диск.")
            else:
                logger.warning("В CSV из Redis не найдено строк для обновления.")
        else:
             logger.error("Не удалось получить клиент Redis или CHAT_ID не установлен. Обновление отменено.")

    # Вывод отчета в консоль (выполняется в любом режиме)
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
    # --- 1. ЗАГРУЗКА КОНФИГУРАЦИИ ---
    keep_unmatched_images = True  # Безопасное значение по умолчанию

    if use_redis:
        logger.info("Загрузка конфигурации из Redis...")
        redis_client = get_redis_client()
        if redis_client:
            config_key = f'{CHAT_ID}:KEEP_UNMATCHED'
            config_value = redis_client.get(config_key)
            if config_value is not None:
                # Redis возвращает строки, приводим к boolean
                keep_unmatched_images = config_value.lower() == 'true'
                logger.info("Настройка KEEP_UNMATCHED из Redis ('%s'): %s", config_key, keep_unmatched_images)
            else:
                logger.warning("Ключ '%s' не найден в Redis. Используется значение по умолчанию: %s", config_key, keep_unmatched_images)
    else:
        logger.info("Загрузка конфигурации из settings.py...")
        # Импортируем локально, чтобы не загружать без необходимости
        from settings import KEEP_UNMATCHED_IMAGES
        keep_unmatched_images = KEEP_UNMATCHED_IMAGES
        logger.info("Настройка KEEP_UNMATCHED из settings.py: %s", keep_unmatched_images)

    # --- 2. ВЫПОЛНЕНИЕ ЭТАПОВ КОНВЕЙЕРА ---

    # Этап 1: Создать кэш
    if not _build_descriptor_cache(use_redis):
        return

    # Этап 2: Обработать изображения, передав загруженную конфигурацию
    final_list = _process_query_images(use_redis, keep_unmatched_images)

    # Этап 3: Обновить источник (если нужно) и вывести отчет
    _update_source_and_report(final_list, use_redis)
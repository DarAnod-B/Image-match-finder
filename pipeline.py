import os
import shutil
from typing import List, Optional

from logger import logger
# Добавляем OUTPUT_DIR из настроек
from settings import DESCRIPTORS_CACHE_PATH, OUTPUT_DIR
from core.cacher import DescriptorCacher
from core.searcher import Searcher

# Модули для работы с данными
import data_loader
from image_link_manager import ImageLinkManager

# Redis
from redis_handler import get_redis_client, CHAT_ID

def _prepare_output_directory(chat_id: str) -> Optional[str]:
    """
    Создает чистую выходную директорию для сессии пользователя.
    Удаляет старые файлы, если они есть.
    """
    try:
        # Создаем уникальный путь для каждого пользователя
        session_output_dir = os.path.join(OUTPUT_DIR, chat_id)
        
        # Удаляем директорию, если она существует, для чистого запуска
        if os.path.exists(session_output_dir):
            shutil.rmtree(session_output_dir)
        
        # Создаем директорию
        os.makedirs(session_output_dir, exist_ok=True)
        logger.info("Создана чистая выходная директория: %s", session_output_dir)
        return session_output_dir
    except Exception as e:
        logger.error("Не удалось создать выходную директорию: %s", e)
        return None


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
    ЭТАП 2: Ищет аналоги, копирует результат (аналог или оригинал) в новую
    директорию с именем, соответствующим индексу, и возвращает список путей к новым файлам.
    """
    logger.info("--- ЭТАП 2: Поиск аналогов и формирование итогового набора изображений ---")
    
    # 0. Готовим выходную директорию
    output_dir = _prepare_output_directory(CHAT_ID)
    if not output_dir:
        return []

    results_with_indices = []
    try:
        searcher = Searcher(DESCRIPTORS_CACHE_PATH)
    except Exception as e:
        logger.error("Критическая ошибка: не удалось загрузить кэш '%s': %s", DESCRIPTORS_CACHE_PATH, e)
        return []

    # 1. ПОЛУЧАЕМ ДАННЫЕ С ИНДЕКСАМИ
    group1_data = data_loader.get_group1_image_paths_with_indices(use_redis_mode)
    if not group1_data:
        logger.warning("В источнике данных для Group 1 не найдено изображений.")
        return []

    # 2. ОБРАБАТЫВАЕМ ИЗОБРАЖЕНИЯ, КОПИРУЕМ И СОХРАНЯЕМ НОВЫЙ ПУТЬ
    for original_index, img1_path in group1_data:
        logger.info("=" * 60)
        logger.info("Обработка позиции %d (файл: %s)", original_index, os.path.basename(img1_path))

        source_path_for_copy = None
        found_match_path = searcher.find_match(img1_path)

        if found_match_path:
            logger.info("✅ Найден аналог: %s", os.path.basename(found_match_path))
            source_path_for_copy = found_match_path
        else:
            logger.warning("⚠️ Аналог не найден.")
            if keep_unmatched_images:
                logger.info("Сохраняем исходное изображение.")
                source_path_for_copy = img1_path
            else:
                logger.info("Отбрасываем изображение.")
                continue  # Пропускаем эту позицию, файл не будет создан

        # 3. КОПИРУЕМ ВЫБРАННЫЙ ФАЙЛ В ВЫХОДНУЮ ДИРЕКТОРИЮ С НОВЫМ ИМЕНЕМ
        try:
            # Получаем расширение исходного файла
            _, extension = os.path.splitext(source_path_for_copy)
            # Создаем новое имя файла на основе индекса (например, 1.jpg, 2.jpg)
            # Используем original_index, т.к. он уникален и по порядку
            destination_name = f"{original_index}{extension}"
            destination_path = os.path.join(output_dir, destination_name)

            # Копируем файл
            shutil.copy2(source_path_for_copy, destination_path)
            logger.info("Файл скопирован в: %s", destination_path)
            
            # Сохраняем кортеж с индексом и ПУТЕМ К НОВОМУ ФАЙЛУ
            results_with_indices.append((original_index, destination_path))

        except Exception as e:
            logger.error("Не удалось скопировать файл %s: %s", source_path_for_copy, e)

    # 4. СОРТИРУЕМ РЕЗУЛЬТАТЫ (хотя они и так должны быть по порядку, это для 100% гарантии)
    results_with_indices.sort(key=lambda item: item[0])
    
    # 5. ФОРМИРУЕМ ФИНАЛЬНЫЙ СПИСОК ПУТЕЙ К НОВЫМ, ОТСОРТИРОВАННЫМ ФАЙЛАМ
    final_list = [path for index, path in results_with_indices]
    
    logger.debug("Сформирован финальный список новых файлов: %s", final_list)
    return final_list


def _update_source_and_report(final_image_list: List[str], use_redis_mode: bool):
    """
    ЭТАП 3: Обновляет источник данных (если Redis) и выводит отчет.
    Эта функция НЕ МЕНЯЕТСЯ, т.к. она просто принимает готовый список путей.
    """
    logger.info("\n--- ЭТАП 3: Обновление источника и формирование отчета ---")

    if use_redis_mode:
        logger.info("Режим: Redis. Обновление CSV в Redis...")
        redis_client = get_redis_client()
        if redis_client and CHAT_ID:
            # ImageLinkManager принимает финальный список и корректно его записывает
            manager = ImageLinkManager.from_redis(redis_client, CHAT_ID)
            if not manager:
                logger.error("Не удалось инициализировать менеджер CSV из Redis.")
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
        else:
             logger.error("Не удалось получить клиент Redis или CHAT_ID не установлен.")

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
    Эта функция НЕ МЕНЯЕТСЯ.
    """
    # ... (код этой функции остается таким же, как у вас)
    # --- 1. ЗАГРУЗКА КОНФИГУРАЦИИ ---
    keep_unmatched_images = True  # Безопасное значение по умолчанию

    if use_redis:
        logger.info("Загрузка конфигурации из Redis...")
        redis_client = get_redis_client()
        if redis_client:
            config_key = f'{CHAT_ID}:KEEP_UNMATCHED'
            config_value = redis_client.get(config_key)
            if config_value is not None:
                keep_unmatched_images = config_value.lower() == 'true'
                logger.info("Настройка KEEP_UNMATCHED из Redis ('%s'): %s", config_key, keep_unmatched_images)
            else:
                logger.warning("Ключ '%s' не найден в Redis. Используется значение по умолчанию: %s", config_key, keep_unmatched_images)
    else:
        logger.info("Загрузка конфигурации из settings.py...")
        from settings import KEEP_UNMATCHED_IMAGES
        keep_unmatched_images = KEEP_UNMATCHED_IMAGES
        logger.info("Настройка KEEP_UNMATCHED из settings.py: %s", keep_unmatched_images)

    # --- 2. ВЫПОЛНЕНИЕ ЭТАПОВ КОНВЕЙЕРА ---
    if not _build_descriptor_cache(use_redis):
        return

    final_list = _process_query_images(use_redis, keep_unmatched_images)

    _update_source_and_report(final_list, use_redis)

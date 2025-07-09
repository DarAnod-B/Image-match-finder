# core/utils.py
"""
Модуль вспомогательных утилит.

Содержит функции общего назначения, которые используются в разных частях
основной логики. Включает в себя:
- Валидацию файлов изображений (проверка на существование, тип и целостность).
- Функции для изменения размера изображений.
- Комплексную функцию для загрузки и предобработки пары изображений для сравнения.
"""
import os
from typing import Tuple, Optional

import cv2
import numpy as np
from PIL import Image, UnidentifiedImageError

from logger import logger
from settings import RESIZE_WIDTH, RESIZE_HEIGHT

# Определяем поддерживаемые расширения на уровне модуля для переиспользования
SUPPORTED_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff', '.webp')


def validate_image_file(file_path: str) -> bool:
    """
    Проверяет, является ли файл действительным и неповрежденным изображением.

    1. Проверяет, что путь существует и является файлом.
    2. Проверяет расширение файла по белому списку.
    3. Пытается открыть и загрузить данные изображения, чтобы убедиться в его целостности.

    Args:
        file_path (str): Путь к файлу для проверки.

    Returns:
        bool: True, если файл является валидным изображением, иначе False.
    """
    # 1. Проверка существования пути
    if not os.path.isfile(file_path):
        logger.debug("Путь не является файлом или не существует: %s", file_path)
        return False
        
    # 2. Проверка расширения
    if not file_path.lower().endswith(SUPPORTED_EXTENSIONS):
        logger.debug("Файл '%s' пропущен из-за неподдерживаемого расширения.", os.path.basename(file_path))
        return False

    # 3. Проверка целостности (самая надежная)
    try:
        # Используем Pillow, так как он отлично справляется с определением целостности
        with Image.open(file_path) as img:
            img.load()  # Попытка загрузить данные изображения в память.
                        # Вызовет исключение для битых или неполных файлов.
        logger.debug("Изображение валидно: %s", file_path)
        return True
    except UnidentifiedImageError:
        logger.warning("Не удалось идентифицировать как изображение (неверный формат): %s", os.path.basename(file_path))
        return False
    except (IOError, OSError) as e:
        logger.warning("Файл поврежден или не может быть прочитан: %s. Ошибка: %s", os.path.basename(file_path), e)
        return False


def resize_image(image: np.ndarray) -> np.ndarray:
    """
    Изменяет размер OpenCV изображения до стандартных размеров проекта.

    Args:
        image (np.ndarray): Исходное изображение в виде NumPy массива.

    Returns:
        np.ndarray: Измененное изображение.
    """
    return cv2.resize(image, (RESIZE_WIDTH, RESIZE_HEIGHT), interpolation=cv2.INTER_AREA)


def load_and_preprocess_images(
    image_path_1: str, image_path_2: str
) -> Optional[Tuple[Image.Image, Image.Image, np.ndarray, np.ndarray]]:
    """
    Комплексная функция для загрузки и подготовки ПАРЫ изображений для сравнения.

    Используется в старых или прямых методах сравнения, где нужны разные форматы.
    Возвращает представления Pillow и OpenCV (в градациях серого и измененного размера).

    Returns:
        Кортеж (img1_pil, img2_pil, img1_gray, img2_gray) или None в случае ошибки.
    """
    try:
        # Загрузка для Pillow (pHash, валидация)
        img1_pil = Image.open(image_path_1)
        img2_pil = Image.open(image_path_2)

        # Загрузка для OpenCV (SSIM, ORB)
        img1_cv = cv2.imread(image_path_1)
        img2_cv = cv2.imread(image_path_2)

        # Критически важная проверка на успешную загрузку OpenCV
        if img1_cv is None or img2_cv is None:
            logger.error("Не удалось загрузить одно или оба изображения с помощью OpenCV: %s, %s", 
                         os.path.basename(image_path_1), os.path.basename(image_path_2))
            return None

        # Предобработка: изменение размера и перевод в серый цвет
        img1_resized = resize_image(img1_cv)
        img2_resized = resize_image(img2_cv)
        
        img1_gray = cv2.cvtColor(img1_resized, cv2.COLOR_BGR2GRAY)
        img2_gray = cv2.cvtColor(img2_resized, cv2.COLOR_BGR2GRAY)

        return img1_pil, img2_pil, img1_gray, img2_gray

    except Exception as e:
        logger.error("Неожиданная ошибка при загрузке и обработке пары изображений: %s", e, exc_info=True)
        return None
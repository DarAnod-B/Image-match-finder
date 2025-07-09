# core/cacher.py
import pickle
import cv2
import numpy as np

from logger import logger # Адаптируем под вашу структуру
from settings import ORB_N_FEATURES
from core.utils import resize_image

def _keypoints_to_json(keypoints):
    """Конвертирует список объектов cv2.KeyPoint в сериализуемый формат."""
    if keypoints is None:
        return []
    return [
        {
            "pt": kp.pt,
            "size": kp.size,
            "angle": kp.angle,
            "response": kp.response,
            "octave": kp.octave,
            "class_id": kp.class_id
        }
        for kp in keypoints
    ]

class DescriptorCacher:
    def __init__(self, image_paths: list[str]):
        self.image_paths = image_paths
        self.orb = cv2.ORB_create(nfeatures=ORB_N_FEATURES)

    def create_and_save_cache(self, cache_path: str):
        """Извлекает и сохраняет дескрипторы и данные о ключевых точках в кэш."""
        logger.info("Создание кэша дескрипторов для %d изображений...", len(self.image_paths))
        
        data_to_cache = {}
        for i, path in enumerate(self.image_paths):
            if (i + 1) % 100 == 0:
                logger.info("Обработано %d/%d изображений...", i + 1, len(self.image_paths))

            try:
                img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
                if img is None: continue
                
                img = resize_image(img)
                kp, des = self.orb.detectAndCompute(img, None)
                
                if des is not None and len(des) > 0:
                    # Сохраняем не сами объекты kp, а их данные
                    data_to_cache[path] = {
                        'kp_data': _keypoints_to_json(kp), 
                        'des': des
                    }
            except Exception as e:
                logger.error("Ошибка при обработке файла %s: %s", path, e)

        logger.info("Кэширование завершено. Сохранение в файл: %s", cache_path)
        with open(cache_path, 'wb') as f:
            pickle.dump(data_to_cache, f)
        
        logger.info("Кэш успешно сохранен.")
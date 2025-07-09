# core/searcher.py
"""
Модуль для поиска изображений с использованием кэша дескрипторов.

Он загружает предварительно вычисленные дескрипторы и данные о ключевых точках,
а затем ищет наилучшее совпадение для каждого нового изображения,
используя Brute-Force сопоставление и геометрическую верификацию RANSAC.
"""
import pickle
import os
import cv2
import numpy as np
from typing import Optional, List, Dict, Any

# Импорты адаптированы под вашу структуру, где logger и settings
# находятся в корне проекта.
from logger import logger
from settings import ORB_N_FEATURES, RANSAC_MIN_INLIERS
from core.utils import resize_image

def _json_to_keypoints(kp_data: List[Dict[str, Any]]) -> List[cv2.KeyPoint]:
    """
    Воссоздает список объектов cv2.KeyPoint из сериализованных данных.
    
    Args:
        kp_data: Список словарей, где каждый словарь описывает один KeyPoint.

    Returns:
        Список объектов cv2.KeyPoint.
    """
    if not kp_data:
        return []
    
    keypoints = []
    for p in kp_data:
        kp = cv2.KeyPoint(
            p["pt"][0],      # x
            p["pt"][1],      # y
            p["size"],       # size
            p["angle"],      # angle
            p["response"],   # response
            p["octave"],     # octave
            p["class_id"]    # class_id
        )
        keypoints.append(kp)
    return keypoints

class Searcher:
    def __init__(self, cache_path: str):
        """
        Инициализирует искатель, загружая кэш дескрипторов.

        Args:
            cache_path (str): Путь к файлу кэша (*.pkl).
        """
        logger.info("Загрузка кэша дескрипторов из '%s'...", cache_path)
        with open(cache_path, 'rb') as f:
            self.cached_data = pickle.load(f)
        
        self.orb = cv2.ORB_create(nfeatures=ORB_N_FEATURES)
        self.matcher = cv2.BFMatcher(cv2.NORM_HAMMING)
        logger.info("Кэш загружен. Найдено %d изображений в базе. Готов к поиску.", len(self.cached_data))

    def find_match(self, query_image_path: str) -> Optional[str]:
        """
        Ищет наилучшее совпадение для query-изображения в кэше.

        Проходит по всем изображениям в кэше, находит то, у которого
        наибольшее количество геометрически согласованных точек, и возвращает путь к нему.

        Returns:
            Путь к найденному изображению или None, если совпадение не найдено.
        """
        # 1. Извлекаем дескрипторы из изображения, которое ищем (query image)
        try:
            query_img_gray = cv2.imread(query_image_path, cv2.IMREAD_GRAYSCALE)
            if query_img_gray is None: 
                logger.warning("Не удалось прочитать query-изображение: %s", query_image_path)
                return None
            
            query_img_gray = resize_image(query_img_gray)
            kp1, des1 = self.orb.detectAndCompute(query_img_gray, None)

            if des1 is None or len(kp1) == 0:
                logger.warning("Не удалось найти дескрипторы в query-изображении: %s", query_image_path)
                return None
        except Exception as e:
            logger.error("Ошибка при обработке query-изображения %s: %s", query_image_path, e)
            return None

        best_candidate_path = None
        max_inliers = -1

        # 2. Итерируемся по всем кэшированным изображениям
        for path, data in self.cached_data.items():
            # Воссоздаем объекты KeyPoint "на лету" из данных кэша
            kp2 = _json_to_keypoints(data['kp_data'])
            des2 = data['des']
            
            if not kp2 or des2 is None:
                continue

            # 3. Быстрое сопоставление с помощью Brute-Force и knn
            try:
                matches = self.matcher.knnMatch(des1, des2, k=2)
            except cv2.error as e:
                logger.debug("Ошибка knnMatch для %s: %s. Пропускаем.", os.path.basename(path), e)
                continue

            # 4. Фильтр Лоу для отбора "хороших" кандидатов
            good_matches = []
            for m, n in matches:
                # Убеждаемся, что есть два соседа для сравнения
                if m and n and m.distance < 0.75 * n.distance:
                    good_matches.append(m)

            # 5. Ранний выход, если кандидатов заведомо мало
            if len(good_matches) < RANSAC_MIN_INLIERS:
                continue

            # 6. Геометрическая верификация (RANSAC)
            src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
            dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
            
            _, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
            
            if mask is None:
                continue
            
            num_inliers = np.sum(mask)

            # 7. Обновляем лучшего кандидата, если текущий результат лучше
            if num_inliers >= RANSAC_MIN_INLIERS and num_inliers > max_inliers:
                max_inliers = num_inliers
                best_candidate_path = path
        
        if best_candidate_path:
            logger.info("Найден лучший кандидат: '%s' с %d согласованными точками.", 
                        os.path.basename(best_candidate_path), max_inliers)

        return best_candidate_path

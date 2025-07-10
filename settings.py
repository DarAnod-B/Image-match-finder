# config/settings.py
"""
Конфигурация для поиска изображений на основе кэширования дескрипторов
и геометрической верификации (RANSAC).
"""
# --- ПУТИ К ДАННЫМ ---
GROUP_1_DIR = r"C:\Programming\Work_project\realto_bot_project\Image_match_finder\test_images\Group1"

# Путь к папке с базой изображений (например, чистые)
GROUP_2_DIR = r"C:\Programming\Work_project\CIAN_general_parser\Project\images\save"

# Теперь мы храним не индекс, а просто кэш дескрипторов
DESCRIPTORS_CACHE_PATH = "descriptors_cache.pkl"

# --- ТЕХНИЧЕСКИЕ ПАРАМЕТРЫ ---
RESIZE_WIDTH = 800
RESIZE_HEIGHT = 800
ORB_N_FEATURES = 2000

# Параметры поиска и верификации
# Минимальное количество "хороших" совпадений для рассмотрения кандидата
MIN_CANDIDATE_MATCHES = 20
# Минимальное количество геометрически согласованных точек (inliers)
RANSAC_MIN_INLIERS = 15

# --- НАСТРОЙКИ ДЛЯ РЕЖИМА REDIS ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

# Параметры парсинга CSV из Redis
CSV_IMAGE_COLUMN = "Ссылки на изображения"
CSV_CELL_DELIMITER = "|"
CSV_IMAGE_DELIMITER = ";"
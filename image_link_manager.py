import csv
import io
import logging
from typing import List, Dict, Any, Iterator, Tuple, Optional
from redis import Redis

class ImageLinkManager:
    """
    Класс для управления данными CSV, в частности ссылками на изображения.
    Может загружать данные из файла или из строки (например, из Redis).
    Инкапсулирует логику парсинга, get/set и сохранения.
    """

    def __init__(self,
                 output_path: str,
                 headers: List[str],
                 rows: List[Dict[str, Any]],
                 image_column_name: str = "Ссылки на изображения",
                 delimiter: str = "|",
                 image_delimiter: str = ";"):
        """
        Приватный конструктор. Используйте from_file() или from_redis() для создания экземпляра.
        """
        self.output_path = output_path
        self.headers = headers
        self.rows = rows
        self.image_column_name = image_column_name
        self.delimiter = delimiter
        self.image_delimiter = image_delimiter

    @classmethod
    def from_redis(cls,
                   redis_client: Redis,
                   chat_id: int,
                   image_column_name: str = "Ссылки на изображения",
                   delimiter: str = "|",
                   image_delimiter: str = ";") -> Optional['ImageLinkManager']:
        """
        Фабричный метод: создает экземпляр ImageLinkManager, загружая данные из Redis.
        """
        logging.info(f"Попытка загрузить CSV данные из Redis для CHAT_ID: {chat_id}")
        try:
            csv_data = redis_client.get(f'{chat_id}:csv:raw')
            output_path = redis_client.get(f"{chat_id}:OUTPUT_FILE_PATH")

            if not csv_data:
                logging.error(f"В Redis не найдены CSV данные по ключу '{chat_id}:csv:raw'")
                return None
            if not output_path:
                logging.error(f"В Redis не найден путь для сохранения по ключу '{chat_id}:OUTPUT_FILE_PATH'")
                return None

            # Используем io.StringIO для чтения строки как файла
            csvfile = io.StringIO(csv_data)
            reader = csv.reader(csvfile, delimiter=delimiter)
            
            headers = next(reader)
            rows = []
            for row_values in reader:
                parsed_row = {}
                for i, header in enumerate(headers):
                    value = row_values[i] if i < len(row_values) else ""
                    if header == image_column_name and value:
                        parsed_row[header] = [link.strip() for link in value.split(image_delimiter) if link.strip()]
                    else:
                        parsed_row[header] = value
                rows.append(parsed_row)
            
            logging.info(f"Успешно загружено и распарсено {len(rows)} строк из Redis.")
            return cls(output_path, headers, rows, image_column_name, delimiter, image_delimiter)

        except Exception as e:
            logging.error(f"Ошибка при загрузке или парсинге данных из Redis: {e}")
            return None

    @classmethod
    def from_file(cls,
                  csv_path: str,
                  image_column_name: str = "Ссылки на изображения",
                  delimiter: str = "|",
                  image_delimiter: str = ";") -> Optional['ImageLinkManager']:
        """
        Фабричный метод: создает экземпляр ImageLinkManager, загружая данные из файла.
        """
        logging.info(f"Попытка загрузить CSV данные из файла: {csv_path}")
        try:
            with open(csv_path, "r", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile, delimiter=delimiter)
                headers = next(reader)
                rows = []
                for row_values in reader:
                    parsed_row = {}
                    for i, header in enumerate(headers):
                        value = row_values[i] if i < len(row_values) else ""
                        if header == image_column_name and value:
                            parsed_row[header] = [link.strip() for link in value.split(image_delimiter) if link.strip()]
                        else:
                            parsed_row[header] = value
                    rows.append(parsed_row)
            
            logging.info(f"Успешно загружено {len(rows)} строк из файла {csv_path}.")
            # При загрузке из файла, сохраняем в тот же файл
            return cls(csv_path, headers, rows, image_column_name, delimiter, image_delimiter)
        except FileNotFoundError:
            logging.error(f"CSV файл не найден: {csv_path}")
            return None
        except Exception as e:
            logging.error(f"Ошибка при чтении CSV файла '{csv_path}': {e}")
            return None
    
    # --- Методы для работы с данными (GET/SET) ---
    
    def iter_rows(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        yield from enumerate(self.rows)

    def get_image_links(self, row_index: int) -> List[str]:
        if 0 <= row_index < len(self.rows):
            return self.rows[row_index].get(self.image_column_name, [])
        return []

    def set_image_links(self, row_index: int, new_links: List[str]):
        if 0 <= row_index < len(self.rows):
            if self.image_column_name in self.headers:
                self.rows[row_index][self.image_column_name] = new_links

    # --- Метод для сохранения ---

    def save_changes_and_get_content(self) -> Optional[str]:
        """
        Собирает CSV в строку, сохраняет ее в файл self.output_path и возвращает эту строку.
        Это позволяет избежать повторного чтения файла для отправки в Redis.
        """
        try:
            # Используем io.StringIO для эффективной сборки CSV в памяти
            string_buffer = io.StringIO()
            writer = csv.DictWriter(string_buffer, fieldnames=self.headers, delimiter=self.delimiter, lineterminator='\n')
            writer.writeheader()
            
            for row in self.rows:
                row_to_write = row.copy()
                if self.image_column_name in row_to_write and isinstance(row_to_write[self.image_column_name], list):
                    row_to_write[self.image_column_name] = self.image_delimiter.join(row_to_write[self.image_column_name])
                writer.writerow(row_to_write)
            
            # Получаем финальный контент из буфера
            updated_csv_content = string_buffer.getvalue()

            # Сохраняем контент в файл
            with open(self.output_path, "w", encoding="utf-8") as f:
                f.write(updated_csv_content)
            
            logging.info(f"✅ CSV файл успешно сохранен по пути: '{self.output_path}'")
            return updated_csv_content

        except Exception as e:
            logging.error(f"Ошибка при сохранении CSV файла '{self.output_path}': {e}")
            return None
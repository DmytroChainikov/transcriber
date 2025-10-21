"""
Google Drive Handler Module

Модуль для роботи з Google Drive - завантаження, вивантаження та моніторинг файлів.
Замінює функціонал локальної файлової системи для роботи з хмарним сховищем.

Usage:
    from google_drive_handler import GoogleDriveHandler
    
    handler = GoogleDriveHandler('credentials.json')
    files = handler.list_files('folder_id')
"""

import os
import io
import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.service_account import Credentials


class GoogleDriveHandler:
    """
    Клас для роботи з Google Drive API
    
    Attributes:
        credentials_file (str): Шлях до файлу з credentials
        service: Сервіс Google Drive API
        verbose (bool): Режим детального виводу інформації
    """
    
    # Область доступу для Google Drive
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]
    
    # MIME типи для різних форматів
    MIME_TYPES = {
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.m4a': 'audio/mp4',
        '.aac': 'audio/aac',
        '.ogg': 'audio/ogg',
        '.txt': 'text/plain',
    }
    
    def __init__(self, credentials_file: str, verbose: bool = False):
        """
        Ініціалізує GoogleDriveHandler
        
        Args:
            credentials_file (str): Шлях до JSON файлу з service account credentials
            verbose (bool): Увімкнути детальний вивід інформації
            
        Raises:
            FileNotFoundError: Якщо credentials файл не знайдено
            Exception: Якщо не вдається авторизуватися
        """
        self.credentials_file = credentials_file
        self.verbose = verbose
        self.service = None
        
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Авторизація в Google Drive API"""
        try:
            credentials = Credentials.from_service_account_file(
                self.credentials_file,
                scopes=self.SCOPES
            )
            self.service = build('drive', 'v3', credentials=credentials)
            
            if self.verbose:
                logging.info("Успішно авторизовано в Google Drive API")
        except FileNotFoundError:
            raise FileNotFoundError(f"Credentials файл не знайдено: {self.credentials_file}")
        except Exception as e:
            raise Exception(f"Помилка авторизації Google Drive: {e}")
    
    def list_files(self, folder_id: str, file_extensions: List[str] = None, 
                   page_size: int = 100) -> List[Dict[str, Any]]:
        """
        Отримує список файлів з папки на Google Drive
        
        Args:
            folder_id (str): ID папки на Google Drive
            file_extensions (List[str]): Список розширень для фільтрації (наприклад, ['.mp3', '.wav'])
            page_size (int): Кількість файлів на сторінку
            
        Returns:
            List[Dict[str, Any]]: Список файлів з метаданими
        """
        try:
            # Формуємо запит для пошуку файлів
            query = f"'{folder_id}' in parents and trashed=false"
            
            # Якщо вказані розширення, додаємо фільтр
            if file_extensions:
                mime_conditions = []
                for ext in file_extensions:
                    if ext in self.MIME_TYPES:
                        mime_type = self.MIME_TYPES[ext]
                        mime_conditions.append(f"mimeType='{mime_type}'")
                
                if mime_conditions:
                    query += f" and ({' or '.join(mime_conditions)})"
            
            # Виконуємо запит
            results = self.service.files().list(
                q=query,
                pageSize=page_size,
                fields="nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime)"
            ).execute()
            
            files = results.get('files', [])
            
            if self.verbose:
                logging.info(f"Знайдено {len(files)} файлів у папці {folder_id}")
            
            return files
            
        except Exception as e:
            logging.error(f"Помилка отримання списку файлів: {e}")
            return []
    
    def download_file(self, file_id: str, destination_path: str) -> bool:
        """
        Завантажує файл з Google Drive
        
        Args:
            file_id (str): ID файлу на Google Drive
            destination_path (str): Локальний шлях для збереження
            
        Returns:
            bool: True якщо завантаження успішне
        """
        try:
            # Створюємо директорію якщо не існує
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            # Запит на завантаження файлу
            request = self.service.files().get_media(fileId=file_id)
            
            # Завантажуємо файл
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if self.verbose and status:
                    logging.info(f"Завантажено {int(status.progress() * 100)}%")
            
            # Записуємо у файл
            with open(destination_path, 'wb') as f:
                fh.seek(0)
                f.write(fh.read())
            
            if self.verbose:
                logging.info(f"Файл завантажено: {destination_path}")
            
            return True
            
        except Exception as e:
            logging.error(f"Помилка завантаження файлу {file_id}: {e}")
            return False
    
    def upload_file(self, file_path: str, folder_id: str, 
                   file_name: str = None) -> Optional[str]:
        """
        Вивантажує файл на Google Drive
        
        Args:
            file_path (str): Локальний шлях до файлу
            folder_id (str): ID папки на Google Drive для завантаження
            file_name (str): Назва файлу (якщо None, використовується ім'я з file_path)
            
        Returns:
            Optional[str]: ID завантаженого файлу або None при помилці
        """
        try:
            if not os.path.exists(file_path):
                logging.error(f"Файл не знайдено: {file_path}")
                return None
            
            # Визначаємо назву файлу
            if not file_name:
                file_name = os.path.basename(file_path)
            
            # Визначаємо MIME тип
            file_extension = Path(file_path).suffix.lower()
            mime_type = self.MIME_TYPES.get(file_extension, 'application/octet-stream')
            
            # Метадані файлу
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            
            # Завантажуємо файл
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name'
            ).execute()
            
            file_id = file.get('id')
            
            if self.verbose:
                logging.info(f"Файл вивантажено на Drive: {file_name} (ID: {file_id})")
            
            return file_id
            
        except Exception as e:
            logging.error(f"Помилка вивантаження файлу {file_path}: {e}")
            return None
    
    def file_exists(self, folder_id: str, file_name: str) -> Optional[str]:
        """
        Перевіряє чи існує файл з вказаною назвою у папці
        
        Args:
            folder_id (str): ID папки на Google Drive
            file_name (str): Назва файлу
            
        Returns:
            Optional[str]: ID файлу якщо існує, None якщо ні
        """
        try:
            query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                return files[0]['id']
            
            return None
            
        except Exception as e:
            logging.error(f"Помилка перевірки існування файлу {file_name}: {e}")
            return None
    
    def get_file_metadata(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Отримує метадані файлу
        
        Args:
            file_id (str): ID файлу
            
        Returns:
            Optional[Dict[str, Any]]: Метадані файлу
        """
        try:
            file = self.service.files().get(
                fileId=file_id,
                fields='id, name, mimeType, size, createdTime, modifiedTime'
            ).execute()
            
            return file
            
        except Exception as e:
            logging.error(f"Помилка отримання метаданих файлу {file_id}: {e}")
            return None
    
    def check_for_new_files(self, folder_id: str, processed_files: set,
                           file_extensions: List[str] = None) -> List[Dict[str, Any]]:
        """
        Перевіряє наявність нових файлів у папці
        
        Args:
            folder_id (str): ID папки на Google Drive
            processed_files (set): Множина ID вже оброблених файлів
            file_extensions (List[str]): Список розширень для фільтрації
            
        Returns:
            List[Dict[str, Any]]: Список нових файлів
        """
        try:
            all_files = self.list_files(folder_id, file_extensions)
            
            # Фільтруємо тільки нові файли
            new_files = []
            for file in all_files:
                file_id = file['id']
                if file_id not in processed_files:
                    new_files.append(file)
            
            if new_files and self.verbose:
                logging.info(f"Знайдено {len(new_files)} нових файлів")
            
            return new_files
            
        except Exception as e:
            logging.error(f"Помилка перевірки нових файлів: {e}")
            return []
    
    def monitor_folder(self, folder_id: str, callback, processed_files: set,
                      file_extensions: List[str] = None, check_interval: int = 60):
        """
        Моніторить папку на наявність нових файлів (polling метод)
        
        Args:
            folder_id (str): ID папки на Google Drive
            callback: Функція для обробки нового файлу
            processed_files (set): Множина ID вже оброблених файлів
            file_extensions (List[str]): Список розширень для фільтрації
            check_interval (int): Інтервал перевірки в секундах
        """
        logging.info(f"Початок моніторингу папки на Google Drive (інтервал: {check_interval}с)")
        
        try:
            while True:
                new_files = self.check_for_new_files(folder_id, processed_files, file_extensions)
                
                for file in new_files:
                    try:
                        # Викликаємо callback для обробки файлу
                        callback(file)
                        
                        # Додаємо до списку оброблених
                        processed_files.add(file['id'])
                        
                    except Exception as e:
                        logging.error(f"Помилка обробки файлу {file['name']}: {e}")
                
                # Чекаємо перед наступною перевіркою
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            logging.info("Моніторинг зупинено користувачем")
        except Exception as e:
            logging.error(f"Помилка моніторингу: {e}")

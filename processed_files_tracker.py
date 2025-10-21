"""
Processed Files Tracker Module

Модуль для відстеження опрацьованих файлів, щоб уникнути повторної обробки.
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, Optional


class ProcessedFilesTracker:
    """Клас для відстеження опрацьованих файлів"""
    
    def __init__(self, tracker_file: str = "processed_files.json"):
        """
        Ініціалізація трекера
        
        Args:
            tracker_file (str): Шлях до файлу з історією
        """
        self.tracker_file = tracker_file
        self.processed_files: Dict[str, Dict] = {}
        self.load_history()
    
    def load_history(self):
        """Завантажує історію з файлу"""
        if os.path.exists(self.tracker_file):
            try:
                with open(self.tracker_file, 'r', encoding='utf-8') as f:
                    self.processed_files = json.load(f)
                logging.info(f"Завантажено історію: {len(self.processed_files)} файлів")
            except Exception as e:
                logging.error(f"Помилка завантаження історії: {e}")
                self.processed_files = {}
        else:
            logging.info("Файл історії не знайдено, створюється новий")
            self.processed_files = {}
    
    def save_history(self):
        """Зберігає історію у файл"""
        try:
            with open(self.tracker_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_files, f, indent=2, ensure_ascii=False)
            logging.debug(f"Історія збережена: {len(self.processed_files)} файлів")
        except Exception as e:
            logging.error(f"Помилка збереження історії: {e}")
    
    def is_processed(self, file_id: str) -> bool:
        """
        Перевіряє чи файл вже оброблено
        
        Args:
            file_id (str): ID файлу (для Drive) або шлях (для локальних файлів)
            
        Returns:
            bool: True якщо файл вже оброблено
        """
        return file_id in self.processed_files
    
    def mark_as_processed(self, file_id: str, file_name: str, 
                         success: bool = True, error: Optional[str] = None,
                         row_number: Optional[int] = None):
        """
        Позначає файл як оброблений
        
        Args:
            file_id (str): ID файлу або шлях
            file_name (str): Назва файлу
            success (bool): Чи успішно оброблено
            error (Optional[str]): Повідомлення про помилку
            row_number (Optional[int]): Номер рядка в таблиці
        """
        self.processed_files[file_id] = {
            'file_name': file_name,
            'processed_at': datetime.now().isoformat(),
            'success': success,
            'error': error,
            'row_number': row_number
        }
        self.save_history()
        
        status = "✅" if success else "❌"
        logging.info(f"{status} Файл позначено як оброблений: {file_name}")
    
    def get_processed_count(self) -> int:
        """Повертає кількість опрацьованих файлів"""
        return len(self.processed_files)
    
    def get_successful_count(self) -> int:
        """Повертає кількість успішно опрацьованих файлів"""
        return sum(1 for f in self.processed_files.values() if f.get('success', False))
    
    def get_failed_count(self) -> int:
        """Повертає кількість невдало опрацьованих файлів"""
        return sum(1 for f in self.processed_files.values() if not f.get('success', True))
    
    def get_file_info(self, file_id: str) -> Optional[Dict]:
        """
        Отримує інформацію про оброблений файл
        
        Args:
            file_id (str): ID файлу
            
        Returns:
            Optional[Dict]: Інформація про файл або None
        """
        return self.processed_files.get(file_id)
    
    def clear_history(self):
        """Очищує всю історію"""
        self.processed_files = {}
        self.save_history()
        logging.info("Історію очищено")
    
    def remove_file(self, file_id: str):
        """
        Видаляє файл з історії (для повторної обробки)
        
        Args:
            file_id (str): ID файлу
        """
        if file_id in self.processed_files:
            file_name = self.processed_files[file_id].get('file_name', file_id)
            del self.processed_files[file_id]
            self.save_history()
            logging.info(f"Файл видалено з історії: {file_name}")
    
    def get_stats(self) -> Dict[str, int]:
        """Повертає статистику обробки"""
        return {
            'total': self.get_processed_count(),
            'successful': self.get_successful_count(),
            'failed': self.get_failed_count()
        }
    
    def print_stats(self):
        """Виводить статистику в лог"""
        stats = self.get_stats()
        logging.info("📊 Статистика обробки файлів:")
        logging.info(f"   Всього оброблено: {stats['total']}")
        logging.info(f"   ✅ Успішно: {stats['successful']}")
        logging.info(f"   ❌ З помилками: {stats['failed']}")

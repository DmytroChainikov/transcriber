import logging
import time
import os
from typing import Set, Dict, Any
from processed_files_tracker import ProcessedFilesTracker


class GoogleDriveFileHandler:
    """Обробник для моніторингу та обробки нових аудіо файлів на Google Drive"""
    
    def __init__(self, transcriber, drive_handler, drive_folder_id: str, 
                 spreadsheet_id: str, worksheet_name: str = None,
                 supported_formats: list = None, max_file_size: int = 20,
                 local_transcripts_folder: str = 'transcripts'):
        """
        Ініціалізує обробник файлів Google Drive
        
        Args:
            transcriber: Екземпляр AudioTranscriber
            drive_handler: Екземпляр GoogleDriveHandler
            drive_folder_id (str): ID папки на Google Drive
            spreadsheet_id (str): ID Google Sheets документу
            worksheet_name (str): Назва аркушу (опційно)
            supported_formats (list): Підтримувані формати файлів
            max_file_size (int): Максимальний розмір файлу в MB
            local_transcripts_folder (str): Папка для локального збереження транскриптів
        """
        self.transcriber = transcriber
        self.drive_handler = drive_handler
        self.drive_folder_id = drive_folder_id
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.supported_formats = [fmt.lower() for fmt in (supported_formats or ['.mp3', '.wav', '.m4a'])]
        self.max_file_size = max_file_size * 1024 * 1024  # Перетворення в байти
        self.processed_files: Set[str] = set()  # Множина ID оброблених файлів
        self.local_transcripts_folder = local_transcripts_folder
        
        # Ініціалізуємо трекер
        self.tracker = ProcessedFilesTracker("processed_files_drive.json")
        logging.info("Ініціалізовано Google Drive File Handler")
        self.tracker.print_stats()
    
    def process_audio_file(self, file_metadata: Dict[str, Any]):
        """
        Обробка аудіо файлу з Google Drive
        
        Args:
            file_metadata (Dict[str, Any]): Метадані файлу з Drive API
        """
        try:
            file_id = file_metadata['id']
            file_name = file_metadata['name']
            file_size = int(file_metadata.get('size', 0))
            
            # Перевірка розміру файлу
            if file_size > self.max_file_size:
                logging.warning(f"Файл {file_name} завеликий ({file_size / 1024 / 1024:.1f} MB)")
                return
            
            # Перевірка чи файл вже був оброблений (через трекер)
            if self.tracker.is_processed(file_id):
                logging.info(f"⏭️ Пропускаємо вже оброблений файл: {file_name}")
                return
            
            # Додаємо до тимчасової множини для поточної сесії
            if file_id in self.processed_files:
                return
            
            logging.info(f"🎵 Обробка нового аудіо файлу з Drive: {file_name}")
            
            # Повна обробка з Google Sheets інтеграцією та локальним збереженням
            result = self.transcriber.process_and_update_sheets(
                audio_file_id=file_id,
                drive_folder_id=self.drive_folder_id,
                spreadsheet_id=self.spreadsheet_id,
                worksheet_name=self.worksheet_name
            )
            
            # Локальне збереження транскрипту
            if result['success'] and result.get('transcript'):
                try:
                    # Створюємо ім'я файлу для локального збереження
                    base_name = file_name.rsplit('.', 1)[0]
                    local_transcript_path = os.path.join(
                        self.local_transcripts_folder, 
                        f"{base_name}_transcript.txt"
                    )
                    
                    # Зберігаємо транскрипт
                    with open(local_transcript_path, 'w', encoding='utf-8') as f:
                        f.write(result['transcript'])
                    
                    logging.info(f"💾 Транскрипт збережено локально: {local_transcript_path}")
                except Exception as e:
                    logging.error(f"Помилка локального збереження транскрипту: {e}")
            
            # Зберігаємо результат в трекері
            if result['success']:
                self.tracker.mark_as_processed(
                    file_id=file_id,
                    file_name=file_name,
                    success=True,
                    row_number=result.get('written_row')
                )
                logging.info(f"✅ Файл оброблено: {file_name}")
                logging.info(f"   📋 Spreadsheet ID: {self.spreadsheet_id}")
                logging.info(f"   📍 Рядок: {result.get('written_row', 'N/A')}")
            else:
                self.tracker.mark_as_processed(
                    file_id=file_id,
                    file_name=file_name,
                    success=False,
                    error=result.get('error')
                )
                logging.error(f"❌ Помилка обробки: {result.get('error', 'Невідома помилка')}")
            
            # Додавання до списку оброблених файлів поточної сесії
            self.processed_files.add(file_id)
            
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {file_metadata.get('name', 'unknown')}: {str(e)}")
    
    def monitor_folder(self, check_interval: int = 60):
        """
        Моніторинг папки Google Drive на нові файли
        
        Args:
            check_interval (int): Інтервал перевірки в секундах
        """
        logging.info(f"Початок моніторингу Google Drive папки (інтервал: {check_interval}с)")
        logging.info(f"Папка ID: {self.drive_folder_id}")
        logging.info(f"Spreadsheet ID: {self.spreadsheet_id}")
        
        try:
            while True:
                # Отримуємо всі аудіо файли з папки
                files = self.drive_handler.list_files(
                    self.drive_folder_id,
                    file_extensions=self.supported_formats
                )
                
                # Обробляємо нові файли
                for file_metadata in files:
                    file_id = file_metadata['id']
                    if file_id not in self.processed_files:
                        self.process_audio_file(file_metadata)
                
                # Чекаємо перед наступною перевіркою
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            logging.info("Моніторинг зупинено користувачем")
        except Exception as e:
            logging.error(f"Помилка моніторингу: {e}")
    
    def process_existing_files(self):
        """
        Обробка всіх існуючих файлів у папці Drive
        """
        logging.info("Початок обробки існуючих файлів на Google Drive")
        
        try:
            files = self.drive_handler.list_files(
                self.drive_folder_id,
                file_extensions=self.supported_formats
            )
            
            total_files = len(files)
            new_files = 0
            skipped_files = 0
            
            logging.info(f"Знайдено {total_files} файлів")
            
            for file_metadata in files:
                file_id = file_metadata['id']
                
                # Перевіряємо чи файл вже оброблено
                if self.tracker.is_processed(file_id):
                    skipped_files += 1
                else:
                    new_files += 1
                    self.process_audio_file(file_metadata)
            
            logging.info(f"✅ Оброблено нових файлів: {new_files}")
            logging.info(f"⏭️ Пропущено (вже оброблених): {skipped_files}")
            logging.info("Обробка існуючих файлів завершена")
            self.tracker.print_stats()
            
        except Exception as e:
            logging.error(f"Помилка при обробці існуючих файлів: {e}")

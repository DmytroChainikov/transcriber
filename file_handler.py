import os
import time
import logging
from pathlib import Path
from watchdog.events import FileSystemEventHandler


class AudioFileHandler(FileSystemEventHandler):
    """Обробник подій файлової системи для моніторингу нових аудіо файлів"""
    
    def __init__(self, transcriber, folder, supported_formats, max_file_size, 
                 excel_file=None, excel_output_dir=None, enable_excel=False):
        self.transcriber = transcriber
        self.folder = folder  # Єдина папка для аудіо та транскриптів
        self.supported_formats = [fmt.lower() for fmt in supported_formats]
        self.max_file_size = max_file_size * 1024 * 1024  # Перетворення в байти
        self.processed_files = set()
        
        # Excel інтеграція
        self.excel_file = excel_file
        self.excel_output_dir = excel_output_dir
        self.enable_excel = enable_excel and excel_file and os.path.exists(excel_file)
    
    def on_created(self, event):
        """Обробка події створення нового файлу"""
        if not event.is_directory:
            self.process_audio_file(event.src_path)
    
    def on_moved(self, event):
        """Обробка події переміщення файлу"""
        if not event.is_directory:
            self.process_audio_file(event.dest_path)
    
    def process_audio_file(self, file_path):
        """
        Обробка аудіо файлу
        
        Args:
            file_path (str): Шлях до аудіо файлу
        """
        try:
            # Перевірка формату файлу
            file_extension = Path(file_path).suffix.lower()
            if file_extension not in self.supported_formats:
                return
            
            # Перевірка що це не текстовий файл транскрибації
            if file_extension == '.txt':
                return
            
            # Перевірка розміру файлу
            file_size = os.path.getsize(file_path)
            if file_size > self.max_file_size:
                logging.warning(f"Файл {file_path} завеликий ({file_size / 1024 / 1024:.1f} MB)")
                return
            
            # Перевірка чи файл вже був оброблений
            if file_path in self.processed_files:
                return
                
            # Перевірка чи транскрипт вже існує
            base_name = Path(file_path).stem
            transcript_file = os.path.join(self.folder, f"{base_name}.txt")
            if os.path.exists(transcript_file):
                logging.info(f"Транскрипт вже існує для: {base_name}")
                self.processed_files.add(file_path)
                return
            
            # Додання невеликої затримки щоб файл повністю записався
            time.sleep(2)
            
            logging.info(f"Обробка нового аудіо файлу: {file_path}")
            
            # Вибираємо режим обробки
            if self.enable_excel:
                # Повна обробка з Excel інтеграцією
                result = self.transcriber.process_and_update_excel(
                    audio_path=file_path,
                    excel_file_path=self.excel_file,
                    output_dir=self.excel_output_dir or os.path.join(self.folder, 'excel_results')
                )
                
                if result['success']:
                    logging.info(f"✅ Файл оброблено з Excel інтеграцією: {os.path.basename(file_path)}")
                    logging.info(f"   📋 Excel: {result.get('updated_excel_file', 'N/A')}")
                    logging.info(f"   📍 Рядок: {result.get('written_row', 'N/A')}")
                else:
                    logging.error(f"❌ Помилка Excel обробки: {result.get('error', 'Невідома помилка')}")
            else:
                # Звичайна транскрибація
                transcript = self.transcriber.transcribe_audio(file_path)
                
                if transcript:
                    # Збереження транскрипту
                    self.transcriber.save_transcript(transcript, transcript_file)
                    logging.info(f"✅ Транскрипт збережено: {os.path.basename(transcript_file)}")
            
            # Додавання до списку оброблених файлів
            self.processed_files.add(file_path)
            
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {file_path}: {str(e)}")
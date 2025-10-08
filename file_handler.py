import os
import time
import logging
from pathlib import Path
from watchdog.events import FileSystemEventHandler


class AudioFileHandler(FileSystemEventHandler):
    """Обробник подій файлової системи для моніторингу нових аудіо файлів"""
    
    def __init__(self, transcriber, folder, supported_formats, max_file_size):
        self.transcriber = transcriber
        self.folder = folder  # Єдина папка для аудіо та транскриптів
        self.supported_formats = [fmt.lower() for fmt in supported_formats]
        self.max_file_size = max_file_size * 1024 * 1024  # Перетворення в байти
        self.processed_files = set()
    
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
            
            # Транскрибація
            transcript = self.transcriber.transcribe_audio(file_path)
            
            if transcript:
                # Збереження транскрипту
                self.transcriber.save_transcript(transcript, transcript_file)
                
                # Додавання до списку оброблених файлів
                self.processed_files.add(file_path)
            
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {file_path}: {str(e)}")
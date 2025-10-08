import os
import time
import logging
from dotenv import load_dotenv
from watchdog.observers import Observer

from transcriber import AudioTranscriber
from file_handler import AudioFileHandler
from utils import process_existing_files

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transcriber.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def load_config():
    """
    Завантаження конфігурації з .env файлу
    
    Returns:
        dict: Словник з налаштуваннями
    """
    load_dotenv()
    
    config = {
        'api_key': os.getenv('GEMINI_API_KEY'),
        'model': os.getenv('GEMINI_MODEL'),
        'folder': os.getenv('AUDIO_FOLDER'),  # Тепер це єдина папка для аудіо та транскриптів
        'supported_formats': os.getenv('SUPPORTED_FORMATS', '.mp3,.wav,.m4a,.aac,.ogg').split(','),
        'max_file_size': int(os.getenv('MAX_FILE_SIZE_MB', 20))
    }
    
    # Валідація конфігурації
    if not config['api_key']:
        raise ValueError("GEMINI_API_KEY не знайдено в .env файлі")
    
    if not config['folder']:
        raise ValueError("AUDIO_FOLDER не знайдено в .env файлі")
    
    return config

def main():
    """Головна функція програми"""
    try:
        # Завантаження конфігурації
        config = load_config()
        logging.info("Конфігурацію завантажено успішно")
        
        # Створення директорії якщо не існує
        os.makedirs(config['folder'], exist_ok=True)
        
        # Ініціалізація транскрибера
        transcriber = AudioTranscriber(config['api_key'], config['model'])
        
        # Обробка існуючих файлів
        logging.info("Початок обробки існуючих файлів...")
        process_existing_files(
            config['folder'],
            transcriber,
            config['supported_formats'],
            config['max_file_size']
        )
        
        # Налаштування моніторингу папки
        event_handler = AudioFileHandler(
            transcriber,
            config['folder'],
            config['supported_formats'],
            config['max_file_size']
        )
        
        observer = Observer()
        observer.schedule(event_handler, config['folder'], recursive=True)
        
        # Запуск моніторингу
        observer.start()
        logging.info(f"Розпочато моніторинг папки: {config['folder']}")
        logging.info("Натисніть Ctrl+C для зупинки програми")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            logging.info("Програму зупинено користувачем")
        
        observer.join()
        
    except Exception as e:
        logging.error(f"Критична помилка: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
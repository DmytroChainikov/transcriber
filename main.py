import os
import time
import logging
import argparse
from dotenv import load_dotenv

from transcriber import AudioTranscriber
from google_drive_file_handler import GoogleDriveFileHandler
from processed_files_tracker import ProcessedFilesTracker

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
        # Gemini API
        'api_key': os.getenv('GEMINI_API_KEY'),
        'model': os.getenv('GEMINI_MODEL', 'gemini-2.5-flash'),
        
        # Google Cloud credentials
        'google_credentials': os.getenv('GOOGLE_CREDENTIALS_PATH'),
        
        # Google Drive & Sheets
        'drive_folder_id': os.getenv('DRIVE_FOLDER_ID'),
        'spreadsheet_id': os.getenv('SPREADSHEET_ID'),
        'worksheet_name': os.getenv('WORKSHEET_NAME'),
        
        # Загальні налаштування
        'supported_formats': os.getenv('SUPPORTED_FORMATS', '.mp3,.wav,.m4a,.aac,.ogg').split(','),
        'max_file_size': int(os.getenv('MAX_FILE_SIZE_MB', 20)),
        'check_interval': int(os.getenv('CHECK_INTERVAL_SEC', 60)),
        
        # Локальне збереження транскриптів
        'local_transcripts_folder': os.getenv('LOCAL_TRANSCRIPTS_FOLDER', 'transcripts')
    }
    
    # Валідація конфігурації
    if not config['api_key']:
        raise ValueError("GEMINI_API_KEY не знайдено в .env файлі")
    
    if not config['google_credentials']:
        raise ValueError("GOOGLE_CREDENTIALS_PATH не знайдено в .env файлі")
    if not config['drive_folder_id']:
        raise ValueError("DRIVE_FOLDER_ID не знайдено в .env файлі")
    if not config['spreadsheet_id']:
        raise ValueError("SPREADSHEET_ID не знайдено в .env файлі")
    
    return config

def main():
    """Головна функція програми"""
    # Парсимо аргументи командного рядка
    parser = argparse.ArgumentParser(description='Audio Transcriber з підтримкою Google Cloud')
    parser.add_argument('--clear-history', action='store_true',
                       help='Очистити історію оброблених файлів')
    parser.add_argument('--show-stats', action='store_true',
                       help='Показати статистику оброблених файлів')
    parser.add_argument('--remove-file', type=str,
                       help='Видалити файл з історії (вказати ID або шлях)')
    args = parser.parse_args()
    
    try:
        # Завантаження конфігурації
        config = load_config()
        
        # Файл трекера для Google Drive режиму
        tracker_file = "processed_files_drive.json"
        
        # Обробка команд керування історією
        if args.show_stats:
            tracker = ProcessedFilesTracker(tracker_file)
            tracker.print_stats()
            return
        
        if args.clear_history:
            response = input("⚠️  Ви впевнені що хочете очистити історію? (yes/no): ")
            if response.lower() == 'yes':
                tracker = ProcessedFilesTracker(tracker_file)
                tracker.clear_history()
                logging.info("✅ Історію очищено")
            else:
                logging.info("❌ Операцію скасовано")
            return
        
        if args.remove_file:
            tracker = ProcessedFilesTracker(tracker_file)
            tracker.remove_file(args.remove_file)
            return
        
        # Звичайний запуск
        logging.info("🌐 Запуск у режимі Google Cloud")
        
        # Ініціалізація транскрибера
        transcriber = AudioTranscriber(
            config['api_key'], 
            config['model'],
            google_credentials_file=config.get('google_credentials')
        )
        
        # Перевірка Google API handlers
        if not transcriber.drive_handler or not transcriber.sheets_handler:
            raise RuntimeError("Не вдалося ініціалізувати Google API handlers")
        
        logging.info(f"   📁 Drive Folder ID: {config['drive_folder_id']}")
        logging.info(f"   📊 Spreadsheet ID: {config['spreadsheet_id']}")
        logging.info(f"   💾 Локальне збереження: {config['local_transcripts_folder']}/")
        
        # Створення папки для локальних транскриптів
        os.makedirs(config['local_transcripts_folder'], exist_ok=True)
        
        # Створюємо file handler для Google Drive
        drive_file_handler = GoogleDriveFileHandler(
            transcriber=transcriber,
            drive_handler=transcriber.drive_handler,
            drive_folder_id=config['drive_folder_id'],
            spreadsheet_id=config['spreadsheet_id'],
            worksheet_name=config.get('worksheet_name'),
            supported_formats=config['supported_formats'],
            max_file_size=config['max_file_size'],
            local_transcripts_folder=config['local_transcripts_folder']
        )
        
        # Обробка існуючих файлів
        logging.info("Обробка існуючих файлів на Google Drive...")
        drive_file_handler.process_existing_files()
        
        # Запуск моніторингу
        logging.info(f"Початок моніторингу Google Drive (інтервал: {config['check_interval']}с)")
        logging.info("Для зупинки натисніть Ctrl+C")
        drive_file_handler.monitor_folder(check_interval=config['check_interval'])
        
    except Exception as e:
        logging.error(f"Критична помилка: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
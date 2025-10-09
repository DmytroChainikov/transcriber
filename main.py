import os
import time
import logging
import glob
from dotenv import load_dotenv
from watchdog.observers import Observer

from transcriber import AudioTranscriber
from file_handler import AudioFileHandler
from utils import process_existing_files
from excel_reader import ExcelDataReader

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
        'max_file_size': int(os.getenv('MAX_FILE_SIZE_MB', 20)),
        'excel_file': os.getenv('EXCEL_TEMPLATE_PATH', 'google_folder/Звіт прослуханих розмов.xlsx'),
        'enable_excel_processing': os.getenv('ENABLE_EXCEL_PROCESSING', 'true').lower() == 'true',
        'excel_output_dir': os.getenv('EXCEL_OUTPUT_DIR', 'excel_output'),
        'batch_processing': os.getenv('ENABLE_BATCH_PROCESSING', 'false').lower() == 'true'
    }
    
    # Валідація конфігурації
    if not config['api_key']:
        raise ValueError("GEMINI_API_KEY не знайдено в .env файлі")
    
    if not config['folder']:
        raise ValueError("AUDIO_FOLDER не знайдено в .env файлі")
    
    return config

def check_excel_template(excel_path):
    """
    Перевіряє доступність Excel шаблону
    
    Args:
        excel_path (str): Шлях до Excel файлу
        
    Returns:
        bool: True якщо файл доступний та коректний
    """
    if not os.path.exists(excel_path):
        logging.warning(f"Excel шаблон не знайдено: {excel_path}")
        return False
    
    try:
        with ExcelDataReader(excel_path, verbose=False) as reader:
            data = reader.read_data()
            if data:
                logging.info(f"Excel шаблон готовий: {len(data)} полів знайдено")
                return True
            else:
                logging.warning("Excel шаблон порожній або має неправильну структуру")
                return False
    except Exception as e:
        logging.error(f"Помилка при перевірці Excel шаблону: {e}")
        return False

def process_single_audio_with_excel(transcriber, audio_path, excel_path, output_dir):
    """
    Обробляє один аудіо файл з Excel інтеграцією
    
    Args:
        transcriber: Екземпляр AudioTranscriber
        audio_path (str): Шлях до аудіо файлу
        excel_path (str): Шлях до Excel шаблону
        output_dir (str): Директорія для збереження результатів
        
    Returns:
        dict: Результат обробки
    """
    try:
        logging.info(f"Початок обробки з Excel: {os.path.basename(audio_path)}")
        
        result = transcriber.process_and_update_excel(
            audio_path=audio_path,
            excel_file_path=excel_path,
            target_row=None,  # Знаходить наступний порожній рядок
            output_excel_path=None,  # Записує в оригінальний файл
            output_dir=output_dir
        )
        
        if result['success']:
            logging.info(f"✅ Файл успішно оброблено: {os.path.basename(audio_path)}")
            logging.info(f"   📋 Excel: {result.get('updated_excel_file', 'N/A')}")
            logging.info(f"   📍 Рядок: {result.get('written_row', 'N/A')}")
        else:
            logging.error(f"❌ Помилка обробки {os.path.basename(audio_path)}: {result.get('error', 'Невідома помилка')}")
        
        return result
        
    except Exception as e:
        logging.error(f"Критична помилка при обробці {audio_path}: {e}")
        return {'success': False, 'error': str(e)}

def run_batch_processing(transcriber, folder, excel_path, supported_formats, output_dir):
    """
    Запускає пакетну обробку всіх аудіо файлів у папці
    
    Args:
        transcriber: Екземпляр AudioTranscriber
        folder (str): Папка з аудіо файлами
        excel_path (str): Шлях до Excel шаблону
        supported_formats (list): Підтримувані формати
        output_dir (str): Директорія для результатів
    """
    logging.info("🚀 Початок пакетної обробки з Excel інтеграцією")
    
    # Знаходимо всі аудіо файли
    audio_files = []
    for format_ext in supported_formats:
        pattern = os.path.join(folder, f"**/*{format_ext}")
        audio_files.extend(glob.glob(pattern, recursive=True))
    
    if not audio_files:
        logging.warning("Аудіо файли для пакетної обробки не знайдено")
        return
    
    # Фільтруємо файли, які вже мають транскрипти
    files_to_process = []
    for audio_file in audio_files:
        transcript_path = os.path.splitext(audio_file)[0] + '.txt'
        if not os.path.exists(transcript_path):
            files_to_process.append(audio_file)
    
    if not files_to_process:
        logging.info("Всі знайдені аудіо файли вже мають транскрипти")
        return
    
    logging.info(f"Знайдено {len(files_to_process)} файлів для обробки")
    
    # Запускаємо пакетну обробку
    batch_result = transcriber.batch_process_audio_files(
        audio_files=files_to_process[:5],  # Обмежуємо до 5 файлів за раз
        excel_file_path=excel_path,
        output_dir=output_dir
    )
    
    # Виводимо результати
    logging.info("📊 Результати пакетної обробки:")
    logging.info(f"   📁 Всього файлів: {batch_result['total_files']}")
    logging.info(f"   ✅ Успішно: {batch_result['successful']}")
    logging.info(f"   ❌ З помилками: {batch_result['failed']}")
    
    if batch_result.get('updated_excel_file'):
        logging.info(f"   📋 Результуючий Excel: {batch_result['updated_excel_file']}")

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
        
        # Перевірка Excel шаблону якщо включена Excel обробка
        excel_available = False
        if config['enable_excel_processing']:
            excel_available = check_excel_template(config['excel_file'])
            if excel_available:
                logging.info("✅ Excel інтеграція активна")
                # Створюємо директорію для Excel результатів
                os.makedirs(config['excel_output_dir'], exist_ok=True)
            else:
                logging.warning("⚠️ Excel інтеграція недоступна, працюємо в звичайному режимі")
        
        # Пакетна обробка якщо включена
        if config['batch_processing'] and excel_available:
            run_batch_processing(
                transcriber,
                config['folder'],
                config['excel_file'],
                config['supported_formats'],
                config['excel_output_dir']
            )
            logging.info("Пакетну обробку завершено. Переходимо до моніторингу...")
        
        # Обробка існуючих файлів (звичайний режим якщо Excel недоступний)
        if not config['batch_processing'] or not excel_available:
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
            config['max_file_size'],
            excel_file=config['excel_file'] if excel_available else None,
            excel_output_dir=config['excel_output_dir'] if excel_available else None,
            enable_excel=excel_available
        )
        
        observer = Observer()
        observer.schedule(event_handler, config['folder'], recursive=True)
        
        # Запуск моніторингу
        observer.start()
        logging.info(f"Розпочато моніторинг папки: {config['folder']}")
        
        # Інформація про режим роботи
        if excel_available:
            logging.info("📊 Режим роботи: Транскрибація + Excel аналіз")
            logging.info(f"   📋 Excel шаблон: {config['excel_file']}")
            logging.info(f"   📁 Результати: {config['excel_output_dir']}")
        else:
            logging.info("📝 Режим роботи: Тільки транскрибація")
        
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
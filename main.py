import os
import time
import logging
from datetime import datetime
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transcriber.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class AudioTranscriber:
    """Клас для транскрибації аудіо файлів з використанням Gemini API"""
    
    def __init__(self, api_key, model_name):
        self.api_key = api_key
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name=model_name)
        logging.info("Ініціалізовано Gemini API")
    
    def transcribe_audio(self, audio_path):
        """
        Транскрибація аудіо файлу
        
        Args:
            audio_path (str): Шлях до аудіо файлу
            
        Returns:
            str: Транскрибований текст або None при помилці
        """
        try:
            # Завантаження аудіо файлу
            audio_file = genai.upload_file(path=audio_path)
            logging.info(f"Завантажено аудіо файл: {audio_path}")
            
            # Промпт для транскрибації
            prompt = """
            Будь ласка, транскрибуй цей аудіо файл українською мовою. 
            Якщо в аудіо звучить інша мова, транскрибуй її оригінальною мовою.
            Збережи структуру мовлення, розділи на абзаци де це доречно.
            """
            
            # Генерація тексту
            response = self.model.generate_content([prompt, audio_file])
            
            # Видалення тимчасового файлу з Gemini
            genai.delete_file(audio_file.name)
            
            if response.text:
                logging.info(f"Успішно транскрибовано: {audio_path}")
                return response.text
            else:
                logging.error(f"Не вдалося отримати текст для файлу: {audio_path}")
                return None
                
        except Exception as e:
            logging.error(f"Помилка при транскрибації {audio_path}: {str(e)}")
            return None
    
    def save_transcript(self, text, output_path):
        """
        Збереження транскрибованого тексту
        
        Args:
            text (str): Текст для збереження
            output_path (str): Шлях для збереження файлу
        """
        try:
            # Створення директорії якщо не існує
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # Додавання заголовка з інформацією
                f.write(f"# Транскрипт\n")
                f.write(f"Дата створення: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("---\n\n")
                f.write(text)
            
            logging.info(f"Збережено транскрипт: {output_path}")
            
        except Exception as e:
            logging.error(f"Помилка при збереженні файлу {output_path}: {str(e)}")

class AudioFileHandler(FileSystemEventHandler):
    """Обробник подій файлової системи для моніторингу нових аудіо файлів"""
    
    def __init__(self, transcriber, output_folder, supported_formats, max_file_size):
        self.transcriber = transcriber
        self.output_folder = output_folder
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
            
            # Перевірка розміру файлу
            file_size = os.path.getsize(file_path)
            if file_size > self.max_file_size:
                logging.warning(f"Файл {file_path} завеликий ({file_size / 1024 / 1024:.1f} MB)")
                return
            
            # Перевірка чи файл вже був оброблений
            if file_path in self.processed_files:
                return
            
            # Додання невеликої затримки щоб файл повністю записався
            time.sleep(2)
            
            logging.info(f"Обробка нового аудіо файлу: {file_path}")
            
            # Транскрибація
            transcript = self.transcriber.transcribe_audio(file_path)
            
            if transcript:
                # Створення назви вихідного файлу
                base_name = Path(file_path).stem
                output_file = os.path.join(self.output_folder, f"{base_name}_transcript.txt")
                
                # Збереження транскрипту
                self.transcriber.save_transcript(transcript, output_file)
                
                # Додавання до списку оброблених файлів
                self.processed_files.add(file_path)
            
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {file_path}: {str(e)}")

def process_existing_files(audio_folder, transcriber, output_folder, supported_formats, max_file_size):
    """
    Обробка існуючих аудіо файлів у папці
    
    Args:
        audio_folder (str): Папка з аудіо файлами
        transcriber (AudioTranscriber): Екземпляр транскрибера
        output_folder (str): Папка для збереження транскриптів
        supported_formats (list): Підтримувані формати файлів
        max_file_size (int): Максимальний розмір файлу в MB
    """
    if not os.path.exists(audio_folder):
        logging.warning(f"Папка {audio_folder} не існує")
        return
    
    max_size_bytes = max_file_size * 1024 * 1024
    supported_formats = [fmt.lower() for fmt in supported_formats]
    
    for root, dirs, files in os.walk(audio_folder):
        for file in files:
            file_path = os.path.join(root, file)
            file_extension = Path(file_path).suffix.lower()
            
            # Перевірка формату та розміру
            if (file_extension in supported_formats and 
                os.path.getsize(file_path) <= max_size_bytes):
                
                # Перевірка чи транскрипт вже існує
                base_name = Path(file_path).stem
                output_file = os.path.join(output_folder, f"{base_name}_transcript.txt")
                
                if not os.path.exists(output_file):
                    logging.info(f"Обробка існуючого файлу: {file_path}")
                    
                    transcript = transcriber.transcribe_audio(file_path)
                    if transcript:
                        transcriber.save_transcript(transcript, output_file)

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
        'audio_folder': os.getenv('AUDIO_FOLDER'),
        'output_folder': os.getenv('OUTPUT_FOLDER'),
        'supported_formats': os.getenv('SUPPORTED_FORMATS', '.mp3,.wav,.m4a,.aac,.ogg').split(','),
        'max_file_size': int(os.getenv('MAX_FILE_SIZE_MB', 20))
    }
    
    # Валідація конфігурації
    if not config['api_key']:
        raise ValueError("GEMINI_API_KEY не знайдено в .env файлі")
    
    if not config['audio_folder']:
        raise ValueError("AUDIO_FOLDER не знайдено в .env файлі")
    
    if not config['output_folder']:
        raise ValueError("OUTPUT_FOLDER не знайдено в .env файлі")
    
    return config

def main():
    """Головна функція програми"""
    try:
        # Завантаження конфігурації
        config = load_config()
        logging.info("Конфігурацію завантажено успішно")
        
        # Створення директорій якщо не існують
        os.makedirs(config['audio_folder'], exist_ok=True)
        os.makedirs(config['output_folder'], exist_ok=True)
        
        # Ініціалізація транскрибера
        transcriber = AudioTranscriber(config['api_key'], config['model'])
        
        # Обробка існуючих файлів
        logging.info("Початок обробки існуючих файлів...")
        process_existing_files(
            config['audio_folder'],
            transcriber,
            config['output_folder'],
            config['supported_formats'],
            config['max_file_size']
        )
        
        # Налаштування моніторингу папки
        event_handler = AudioFileHandler(
            transcriber,
            config['output_folder'],
            config['supported_formats'],
            config['max_file_size']
        )
        
        observer = Observer()
        observer.schedule(event_handler, config['audio_folder'], recursive=True)
        
        # Запуск моніторингу
        observer.start()
        logging.info(f"Розпочато моніторинг папки: {config['audio_folder']}")
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
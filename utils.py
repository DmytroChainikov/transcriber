import os
import logging
from pathlib import Path


def process_existing_files(folder, transcriber, supported_formats, max_file_size):
    """
    Обробка існуючих аудіо файлів у папці
    
    Args:
        folder (str): Папка з аудіо файлами та для збереження транскриптів
        transcriber (AudioTranscriber): Екземпляр транскрибера
        supported_formats (list): Підтримувані формати файлів
        max_file_size (int): Максимальний розмір файлу в MB
    """
    if not os.path.exists(folder):
        logging.warning(f"Папка {folder} не існує")
        return
    
    max_size_bytes = max_file_size * 1024 * 1024
    supported_formats = [fmt.lower() for fmt in supported_formats]
    
    # Отримуємо всі файли в папці
    all_files = os.listdir(folder)
    
    # Фільтруємо аудіо файли
    audio_files = []
    for file in all_files:
        file_path = os.path.join(folder, file)
        if os.path.isfile(file_path):
            file_extension = Path(file_path).suffix.lower()
            
            # Перевірка формату та розміру
            if (file_extension in supported_formats and 
                os.path.getsize(file_path) <= max_size_bytes):
                audio_files.append(file_path)
    
    # Перевіряємо кожен аудіо файл на наявність транскрипції
    for audio_file in audio_files:
        base_name = Path(audio_file).stem
        transcript_file = os.path.join(folder, f"{base_name}.txt")
        
        # Якщо транскрипт не існує - створюємо
        if not os.path.exists(transcript_file):
            logging.info(f"Обробка файлу без транскрипції: {audio_file}")
            
            transcript = transcriber.transcribe_audio(audio_file)
            if transcript:
                transcriber.save_transcript(transcript, transcript_file)
        else:
            logging.info(f"Транскрипт вже існує для: {base_name}")
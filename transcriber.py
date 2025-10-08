import os
import logging
from datetime import datetime
import google.generativeai as genai


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
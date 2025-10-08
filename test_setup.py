#!/usr/bin/env python3
"""
Тестовий скрипт для перевірки налаштувань транскрибера
"""

import os
import sys
from dotenv import load_dotenv
import google.generativeai as genai

def test_configuration():
    """Тестування конфігурації"""
    print("🔧 Перевірка конфігурації...")
    
    # Завантаження .env
    if not os.path.exists('.env'):
        print("❌ Файл .env не знайдено!")
        print("   Скопіюйте .env.example як .env та заповніть своїми даними")
        return False
    
    load_dotenv()
    
    # Перевірка API ключа
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key or api_key == 'your_gemini_api_key_here':
        print("❌ GEMINI_API_KEY не налаштований!")
        print("   Отримайте ключ: https://aistudio.google.com/app/apikey")
        return False
    
    
    # Перевірка папок
    audio_folder = os.getenv('AUDIO_FOLDER')
    output_folder = os.getenv('OUTPUT_FOLDER')
    
    if not audio_folder:
        print("❌ AUDIO_FOLDER не налаштований!")
        return False
    
    if not output_folder:
        print("❌ OUTPUT_FOLDER не налаштований!")
        return False
    
    print(f"✅ API ключ: {'*' * 10 + api_key[-4:] if len(api_key) > 4 else '****'}")
    print(f"✅ Папка аудіо: {audio_folder}")
    print(f"✅ Папка виводу: {output_folder}")
    
    return True, api_key, audio_folder, output_folder

def test_gemini_api(api_key):
    """Тестування з'єднання з Gemini API"""
    print("\n🌐 Перевірка з'єднання з Gemini API...")
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Простий тест
        response = model.generate_content("Привіт! Відповідь одним словом: Працює")
        if response.text:
            print("✅ З'єднання з Gemini API успішне!")
            print(f"   Тестова відповідь: {response.text.strip()}")
            return True
        else:
            print("❌ Не вдалося отримати відповідь від API")
            return False
            
    except Exception as e:
        print(f"❌ Помилка API: {str(e)}")
        return False

def test_folders(audio_folder, output_folder):
    """Перевірка та створення папок"""
    print("\n📁 Перевірка папок...")
    
    try:
        # Створення папок якщо не існують
        os.makedirs(audio_folder, exist_ok=True)
        os.makedirs(output_folder, exist_ok=True)
        
        # Перевірка доступу на запис
        test_file = os.path.join(output_folder, 'test_write.tmp')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        
        print(f"✅ Папка аудіо: {audio_folder} (доступна)")
        print(f"✅ Папка виводу: {output_folder} (доступна для запису)")
        return True
        
    except Exception as e:
        print(f"❌ Помилка з папками: {str(e)}")
        return False

def main():
    """Головна функція тесту"""
    print("🧪 Тестування Audio Transcriber")
    print("=" * 40)
    
    # Тест конфігурації
    config_result = test_configuration()
    if not config_result:
        print("\n❌ Тест конфігурації не пройдено!")
        return 1
    
    _, api_key, audio_folder, output_folder = config_result
    
    # Тест API
    if not test_gemini_api(api_key):
        print("\n❌ Тест API не пройдено!")
        return 1
    
    # Тест папок
    if not test_folders(audio_folder, output_folder):
        print("\n❌ Тест папок не пройдено!")
        return 1
    
    print("\n" + "=" * 40)
    print("🎉 Всі тести пройдено успішно!")
    print("   Можете запускати main.py для початку роботи")
    print("   Команда: python main.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
import os
import logging
from datetime import datetime
import json
from typing import Dict, List, Any, Optional, Union
import google.generativeai as genai
from excel_reader import ExcelDataReader
from excel_writer import write_analyzed_data_to_excel


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
    
    def analyze_text_for_field(self, text: str, field_name: str, field_info: Dict[str, Any]) -> Any:
        """
        Аналізує текст за допомогою Gemini для отримання значення конкретного поля
        (ЗАСТАРІЛИЙ МЕТОД - використовуйте fill_excel_data для оптимізації API викликів)
        
        Args:
            text (str): Текст для аналізу
            field_name (str): Назва поля
            field_info (Dict[str, Any]): Інформація про поле з excel_reader
            
        Returns:
            Any: Значення поля або None при помилці
        """
        logging.warning("Використовується неоптимізований метод analyze_text_for_field. Рекомендується використовувати fill_excel_data.")
        
        try:
            if field_info.get('type') == 'dropdown':
                return self._analyze_dropdown_field(text, field_name, field_info)
            else:
                return self._analyze_text_field(text, field_name, field_info)
                
        except Exception as e:
            logging.error(f"Помилка при аналізі поля {field_name}: {str(e)}")
            return None
    
    def _analyze_dropdown_field(self, text: str, field_name: str, field_info: Dict[str, Any]) -> Optional[str]:
        """
        Аналізує текст для dropdown поля
        
        Args:
            text (str): Текст для аналізу
            field_name (str): Назва поля
            field_info (Dict[str, Any]): Інформація про поле
            
        Returns:
            Optional[str]: Вибране значення з dropdown або None
        """
        dropdown_options = field_info.get('dropdown_options', [])
        if not dropdown_options:
            return None
        
        options_str = ", ".join(dropdown_options)
        
        prompt = f"""
        Проаналізуй наступний текст та визнач, яке з доступних значень найбільше підходить для поля "{field_name}".

        Текст для аналізу:
        {text}

        Доступні варіанти для поля "{field_name}": {options_str}

        ВАЖЛИВО: 
        - Повернити ТІЛЬКИ одне зі значень зі списку доступних варіантів
        - Якщо жоден варіант не підходить, поверни "Невизначено"
        - Не додавай жодних пояснень, тільки значення
        - Відповідь має бути точно такою, як у списку варіантів

        Відповідь:
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            # Перевіряємо чи результат є в списку доступних варіантів
            if result in dropdown_options:
                logging.info(f"Для поля '{field_name}' обрано: {result}")
                return result
            else:
                logging.warning(f"Результат '{result}' не знайдено в доступних варіантах для поля '{field_name}'")
                return dropdown_options[0] if dropdown_options else None
                
        except Exception as e:
            logging.error(f"Помилка при аналізі dropdown поля {field_name}: {str(e)}")
            return dropdown_options[0] if dropdown_options else None
    
    def _analyze_text_field(self, text: str, field_name: str, field_info: Dict[str, Any]) -> Optional[str]:
        """
        Аналізує текст для звичайного текстового поля
        
        Args:
            text (str): Текст для аналізу
            field_name (str): Назва поля
            field_info (Dict[str, Any]): Інформація про поле
            
        Returns:
            Optional[str]: Значення поля
        """
        prompt = f"""
        Проаналізуй наступний текст та витягни інформацію для поля "{field_name}".

        Текст для аналізу:
        {text}

        Поле: "{field_name}"

        ВАЖЛИВО:
        - Дай коротку, конкретну відповідь (максимум 2-3 речення)
        - Якщо інформація відсутня, поверни "Не вказано"
        - Не додавай зайвих пояснень

        Відповідь:
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            logging.info(f"Для поля '{field_name}' отримано: {result}")
            return result
            
        except Exception as e:
            logging.error(f"Помилка при аналізі текстового поля {field_name}: {str(e)}")
            return "Помилка аналізу"
    
    def _analyze_all_dropdown_fields(self, text: str, dropdown_fields: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """
        Аналізує всі dropdown поля одним запитом для економії API викликів
        
        Args:
            text (str): Текст для аналізу
            dropdown_fields (Dict[str, Dict[str, Any]]): Словник dropdown полів
            
        Returns:
            Dict[str, str]: Результати для кожного dropdown поля
        """
        if not dropdown_fields:
            return {}
        
        # Створюємо один великий промпт для всіх dropdown полів
        prompt_parts = [
            "Проаналізуй наступний текст та визнач найбільш підходящі значення для кожного поля з випадаючого списку.",
            f"\nТекст для аналізу:\n{text}",
            "\nПоля та їх варіанти:"
        ]
        
        field_mappings = {}
        for i, (field_name, field_info) in enumerate(dropdown_fields.items(), 1):
            dropdown_options = field_info.get('dropdown_options', [])
            if dropdown_options:
                options_str = ", ".join(dropdown_options)
                prompt_parts.append(f"{i}. {field_name}: [{options_str}]")
                field_mappings[i] = field_name
        
        prompt_parts.extend([
            "\nВАЖЛИВО:",
            "- Для кожного поля оберіть ТІЛЬКИ одне значення зі списку варіантів",
            "- Якщо варіант не підходить, використайте перший варіант зі списку",
            "- Відповідь надайте у форматі: '1: [значення], 2: [значення], ...'",
            "- Використовуйте точно ті значення, що є у списках варіантів",
            "\nВідповідь:"
        ])
        
        full_prompt = "\n".join(prompt_parts)
        
        try:
            response = self.model.generate_content(full_prompt)
            result_text = response.text.strip()
            
            logging.info(f"Отримано відповідь для {len(dropdown_fields)} dropdown полів: {result_text}")
            
            # Парсимо відповідь
            results = {}
            for line in result_text.split(','):
                line = line.strip()
                if ':' in line:
                    try:
                        field_num_str, value = line.split(':', 1)
                        field_num = int(field_num_str.strip())
                        value = value.strip()
                        
                        if field_num in field_mappings:
                            field_name = field_mappings[field_num]
                            field_info = dropdown_fields[field_name]
                            dropdown_options = field_info.get('dropdown_options', [])
                            
                            # Перевіряємо чи значення є в списку варіантів
                            if value in dropdown_options:
                                results[field_name] = value
                                logging.info(f"Dropdown поле '{field_name}': {value}")
                            else:
                                # Шукаємо найближче значення або берemо перше
                                fallback_value = dropdown_options[0] if dropdown_options else "Невизначено"
                                results[field_name] = fallback_value
                                logging.warning(f"Значення '{value}' не знайдено для поля '{field_name}', використано: {fallback_value}")
                    except (ValueError, IndexError) as e:
                        logging.error(f"Помилка парсингу рядка '{line}': {e}")
                        continue
            
            # Заповнюємо поля що не були оброблені
            for field_name, field_info in dropdown_fields.items():
                if field_name not in results:
                    dropdown_options = field_info.get('dropdown_options', [])
                    fallback_value = dropdown_options[0] if dropdown_options else "Невизначено"
                    results[field_name] = fallback_value
                    logging.warning(f"Поле '{field_name}' не оброблено, використано значення за замовчуванням: {fallback_value}")
            
            return results
            
        except Exception as e:
            logging.error(f"Помилка при аналізі dropdown полів: {str(e)}")
            # Повертаємо значення за замовчуванням для всіх полів
            results = {}
            for field_name, field_info in dropdown_fields.items():
                dropdown_options = field_info.get('dropdown_options', [])
                results[field_name] = dropdown_options[0] if dropdown_options else "Невизначено"
            return results
    
    def _analyze_all_text_fields(self, text: str, text_fields: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """
        Аналізує всі текстові поля одним запитом для економії API викликів
        
        Args:
            text (str): Текст для аналізу
            text_fields (Dict[str, Dict[str, Any]]): Словник текстових полів
            
        Returns:
            Dict[str, str]: Результати для кожного текстового поля
        """
        if not text_fields:
            return {}
        
        # Створюємо один запит для всіх текстових полів
        prompt_parts = [
            "Проаналізуй наступний текст та витягни інформацію для кожного з наступних полів.",
            f"\nТекст для аналізу:\n{text}",
            "\nПоля для заповнення:"
        ]
        
        field_mappings = {}
        for i, field_name in enumerate(text_fields.keys(), 1):
            prompt_parts.append(f"{i}. {field_name}")
            field_mappings[i] = field_name
        
        prompt_parts.extend([
            "\nВАЖЛИВО:",
            "- Для кожного поля дайте коротку, конкретну відповідь (максимум 2-3 речення)",
            "- Якщо інформація відсутня, напишіть 'Не вказано'",
            "- Відповідь надайте у форматі: '1: [відповідь], 2: [відповідь], ...'",
            "- Не додавайте зайвих пояснень",
            "\nВідповідь:"
        ])
        
        full_prompt = "\n".join(prompt_parts)
        
        try:
            response = self.model.generate_content(full_prompt)
            result_text = response.text.strip()
            
            logging.info(f"Отримано відповідь для {len(text_fields)} текстових полів")
            
            # Парсимо відповідь
            results = {}
            for line in result_text.split(','):
                line = line.strip()
                if ':' in line:
                    try:
                        field_num_str, value = line.split(':', 1)
                        field_num = int(field_num_str.strip())
                        value = value.strip()
                        
                        if field_num in field_mappings:
                            field_name = field_mappings[field_num]
                            results[field_name] = value
                            logging.info(f"Текстове поле '{field_name}': {value}")
                    except (ValueError, IndexError) as e:
                        logging.error(f"Помилка парсингу рядка '{line}': {e}")
                        continue
            
            # Заповнюємо поля що не були оброблені
            for field_name in text_fields.keys():
                if field_name not in results:
                    results[field_name] = "Не вказано"
                    logging.warning(f"Поле '{field_name}' не оброблено, використано: 'Не вказано'")
            
            return results
            
        except Exception as e:
            logging.error(f"Помилка при аналізі текстових полів: {str(e)}")
            # Повертаємо значення за замовчуванням для всіх полів
            return {field_name: "Помилка аналізу" for field_name in text_fields.keys()}

    def _evaluate_manager_performance(self, transcript_text: str) -> Dict[str, Any]:
        """
        Оцінює якість роботи менеджера на основі транскрипту
        
        Args:
            transcript_text (str): Текст розмови для аналізу
            
        Returns:
            Dict[str, Any]: Результат оцінки з коментарями та рекомендаціями
        """
        try:
            prompt = f"""
            Проаналізуй розмову менеджера з клієнтом та оціни якість обслуговування.

            Текст розмови:
            {transcript_text}

            Оціни наступні аспекти (відповідай ТІЛЬКИ 1 або 0):
            1. Ввічливість менеджера (1 - ввічливий, 0 - неввічливий)
            2. Професійність відповідей (1 - професійно, 0 - непрофесійно) 
            3. Швидкість реагування (1 - швидко відповідав, 0 - повільно)
            4. Вирішення питань клієнта (1 - вирішив, 0 - не вирішив)
            5. Дотримання протоколу (1 - дотримувався, 0 - порушував)

            ФОРМАТ ВІДПОВІДІ:
            Ввічливість: [1/0]
            Професійність: [1/0]  
            Швидкість: [1/0]
            Вирішення: [1/0]
            Протокол: [1/0]
            Загальна оцінка: [коротка оцінка якості]
            Рекомендації: [короткі рекомендації якщо є проблеми]
            """
            
            response = self.model.generate_content(prompt)
            result_text = response.text.strip()
            
            # Парсимо результат
            evaluation = {
                'scores': {},
                'total_score': 0,
                'overall_assessment': '',
                'recommendations': '',
                'is_performance_good': True
            }
            
            lines = result_text.split('\n')
            for line in lines:
                line = line.strip()
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    
                    if key in ['ввічливість', 'професійність', 'швидкість', 'вирішення', 'протокол']:
                        try:
                            score = int(value)
                            evaluation['scores'][key] = score
                            evaluation['total_score'] += score
                        except ValueError:
                            evaluation['scores'][key] = 0
                    elif 'загальна оцінка' in key:
                        evaluation['overall_assessment'] = value
                    elif 'рекомендації' in key:
                        evaluation['recommendations'] = value
            
            # Визначаємо чи є проблеми (менше 4 балів з 5 або є негативні коментарі)
            evaluation['is_performance_good'] = evaluation['total_score'] >= 4 and 'проблем' not in evaluation['recommendations'].lower()
            
            logging.info(f"Оцінка менеджера: {evaluation['total_score']}/5, Якість: {'Добра' if evaluation['is_performance_good'] else 'Потребує покращення'}")
            
            return evaluation
            
        except Exception as e:
            logging.error(f"Помилка при оцінці роботи менеджера: {str(e)}")
            return {
                'scores': {},
                'total_score': 0,
                'overall_assessment': 'Помилка аналізу',
                'recommendations': 'Не вдалося проаналізувати',
                'is_performance_good': True
            }

    def _calculate_score_from_binary_fields(self, filled_data: Dict[str, Any]) -> int:
        """
        Підраховує загальний бал на основі полів зі значеннями 1/0
        
        Args:
            filled_data (Dict[str, Any]): Заповнені дані з полями
            
        Returns:
            int: Загальна кількість балів
        """
        total_score = 0
        binary_fields_count = 0
        
        for field_name, field_info in filled_data.items():
            if isinstance(field_info, dict):
                value = field_info.get('analyzed_value', '')
                
                # Шукаємо поля що містять оцінки 1/0
                if isinstance(value, str):
                    if value.strip() in ['1', '0']:
                        try:
                            score = int(value.strip())
                            total_score += score
                            binary_fields_count += 1
                            logging.info(f"Поле '{field_name}': {score} бал")
                        except ValueError:
                            continue
        
        logging.info(f"Загальний бал: {total_score}/{binary_fields_count}")
        return total_score

    def fill_excel_data(self, transcript_text: str, excel_file_path: str, 
                       header_row: int = 2, target_row: int = 3) -> Dict[str, Any]:
        """
        Заповнює Excel таблицю на основі транскрипту та структури полів (оптимізована версія)
        
        Args:
            transcript_text (str): Транскрибований текст
            excel_file_path (str): Шлях до Excel файлу
            header_row (int): Номер рядка з заголовками
            target_row (int): Номер рядка для заповнення
            
        Returns:
            Dict[str, Any]: Заповнені дані
        """
        try:
            # Читаємо структуру полів з Excel
            with ExcelDataReader(excel_file_path, verbose=True) as reader:
                fields_structure = reader.read_data(header_row=header_row, data_row=target_row)
            
            filled_data = {}
            
            logging.info(f"Почато оптимізоване заповнення {len(fields_structure)} полів")
            
            # Розділяємо поля за типами для групової обробки
            dropdown_fields = {}
            text_fields = {}
            
            for field_name, field_info in fields_structure.items():
                if field_info is None:
                    continue
                    
                if field_info.get('type') == 'dropdown' and field_info.get('dropdown_options'):
                    dropdown_fields[field_name] = field_info
                else:
                    text_fields[field_name] = field_info
            
            api_calls_count = 0
            
            # Обробляємо всі dropdown поля одним запитом
            if dropdown_fields:
                logging.info(f"Аналіз {len(dropdown_fields)} dropdown полів одним запитом")
                dropdown_results = self._analyze_all_dropdown_fields(transcript_text, dropdown_fields)
                api_calls_count += 1
                
                for field_name, result in dropdown_results.items():
                    filled_data[field_name] = {
                        'original_info': dropdown_fields[field_name],
                        'analyzed_value': result,
                        'field_type': 'dropdown'
                    }
            
            # Обробляємо всі текстові поля одним запитом
            if text_fields:
                logging.info(f"Аналіз {len(text_fields)} текстових полів одним запитом")
                text_results = self._analyze_all_text_fields(transcript_text, text_fields)
                api_calls_count += 1
                
                for field_name, result in text_results.items():
                    filled_data[field_name] = {
                        'original_info': text_fields[field_name],
                        'analyzed_value': result,
                        'field_type': text_fields[field_name].get('type', 'text')
                    }
            
            # Додаткова обробка: оцінка менеджера та підрахунок балів
            manager_evaluation = self._evaluate_manager_performance(transcript_text)
            api_calls_count += 1  # Додатковий виклик для оцінки
            
            # Додаємо оцінку менеджера в результат
            filled_data['_manager_evaluation'] = {
                'original_info': {'type': 'evaluation'},
                'analyzed_value': manager_evaluation,
                'field_type': 'evaluation'
            }
            
            # Підраховуємо загальний бал з полів 1/0
            total_score = self._calculate_score_from_binary_fields(filled_data)
            
            # Додаємо підрахований бал як службове поле для використання в excel_writer
            filled_data['_total_score'] = {
                'original_info': {'type': 'calculated'},
                'analyzed_value': total_score,
                'field_type': 'calculated'
            }
            
            # Позначаємо поле "Оцінка" для автоматичного заповнення
            for field_name, field_info in filled_data.items():
                if field_name not in ['_manager_evaluation', '_total_score'] and isinstance(field_info, dict):
                    # Шукаємо поле "Оцінка" або схожі
                    if any(keyword in field_name.lower() for keyword in ['оцінка', 'оценка', 'score', 'rating']):
                        filled_data[field_name]['calculated_score'] = True
                        logging.info(f"Поле '{field_name}' позначено для автоматичного заповнення балом: {total_score}")
                        break
            
            logging.info(f"Заповнення завершено за {api_calls_count} API викликів (включно з оцінкою менеджера)")
            logging.info(f"Економія: {len(fields_structure) - api_calls_count + 1} викликів")
            return filled_data
            
        except Exception as e:
            logging.error(f"Помилка при заповненні Excel даних: {str(e)}")
            return {}
    
    def process_audio_and_fill_excel(self, audio_path: str, excel_file_path: str, 
                                   output_dir: str = "output") -> Dict[str, str]:
        """
        Повний процес: транскрибація аудіо + заповнення Excel таблиці
        
        Args:
            audio_path (str): Шлях до аудіо файлу
            excel_file_path (str): Шлях до Excel файлу з структурою
            output_dir (str): Директорія для збереження результатів
            
        Returns:
            Dict[str, str]: Шляхи до створених файлів
        """
        try:
            # Створюємо вихідну директорію
            os.makedirs(output_dir, exist_ok=True)
            
            # Отримуємо базову назву файлу
            base_name = os.path.splitext(os.path.basename(audio_path))[0]
            
            # Шляхи для вихідних файлів
            transcript_path = os.path.join(output_dir, f"{base_name}_transcript.txt")
            
            results = {
                'audio_file': audio_path,
                'transcript_file': None,
                'success': False
            }
            
            # Крок 1: Транскрибація аудіо
            logging.info(f"Початок транскрибації: {audio_path}")
            transcript = self.transcribe_audio(audio_path)
            
            if not transcript:
                logging.error("Не вдалося отримати транскрипт")
                return results
            
            # Збереження транскрипту
            self.save_transcript(transcript, transcript_path)
            results['transcript_file'] = transcript_path
            
            # Крок 2: Заповнення Excel даних
            logging.info(f"Початок заповнення Excel даних: {excel_file_path}")
            filled_data = self.fill_excel_data(transcript, excel_file_path)
            
            if not filled_data:
                logging.error("Не вдалося заповнити дані")
                return results
            
            logging.info(f"Процес завершено успішно для файлу: {audio_path}")
            return results
            
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {audio_path}: {str(e)}")
            return results
    
    def process_and_update_excel(self, audio_path: str, excel_file_path: str, 
                                target_row: Optional[int] = None, 
                                output_excel_path: Optional[str] = None,
                                output_dir: str = "output") -> Dict[str, Any]:
        """
        Повний процес: транскрибація + заповнення + оновлення Excel файлу
        
        Args:
            audio_path (str): Шлях до аудіо файлу
            excel_file_path (str): Шлях до Excel файлу з структурою
            target_row (Optional[int]): Конкретний рядок для заповнення (якщо None, додає новий)
            output_excel_path (Optional[str]): Шлях для збереження оновленого Excel (якщо None, перезаписує оригінал)
            output_dir (str): Директорія для збереження допоміжних файлів
            
        Returns:
            Dict[str, Any]: Результат операції з шляхами до файлів
        """
        try:
            # Створюємо вихідну директорію
            os.makedirs(output_dir, exist_ok=True)
            
            # Отримуємо базову назву файлу
            base_name = os.path.splitext(os.path.basename(audio_path))[0]
            
            results = {
                'audio_file': audio_path,
                'transcript_file': None,
                'updated_excel_file': None,
                'written_row': None,
                'success': False,
                'error': None
            }
            
            # Крок 1: Транскрибація аудіо
            logging.info(f"Початок транскрибації: {audio_path}")
            transcript = self.transcribe_audio(audio_path)
            
            if not transcript:
                results['error'] = "Не вдалося отримати транскрипт"
                return results
            
            # Збереження транскрипту
            transcript_path = os.path.join(output_dir, f"{base_name}_transcript.txt")
            self.save_transcript(transcript, transcript_path)
            results['transcript_file'] = transcript_path
            
            # Крок 2: Аналіз та заповнення даних
            logging.info(f"Початок аналізу та заповнення: {excel_file_path}")
            filled_data = self.fill_excel_data(transcript, excel_file_path)
            
            if not filled_data:
                results['error'] = "Не вдалося проаналізувати дані"
                return results
            
            # Крок 3: Оновлення Excel файлу
            logging.info("Початок оновлення Excel файлу")
            
            # Визначаємо шлях для збереження Excel
            if not output_excel_path:
                excel_dir = os.path.dirname(excel_file_path)
                excel_name = os.path.splitext(os.path.basename(excel_file_path))[0]
                output_excel_path = os.path.join(excel_dir, f"{excel_name}_updated.xlsx")
            
            # Записуємо дані у Excel
            write_result = write_analyzed_data_to_excel(
                excel_file_path=excel_file_path,
                analyzed_data=filled_data,
                output_file_path=None,  # Завжди записуємо в оригінальний файл
                target_row=target_row,
                filename=audio_path,
                verbose=True
            )
            
            if write_result['success']:
                results['updated_excel_file'] = write_result['output_file']
                results['written_row'] = write_result['written_row']
                results['success'] = True
                
                logging.info(f"✅ Повний процес завершено успішно!")
                logging.info(f"📝 Транскрипт: {transcript_path}")
                logging.info(f" Оновлений Excel: {output_excel_path}")
                logging.info(f"📍 Заповнено рядок: {write_result['written_row']}")
            else:
                results['error'] = f"Помилка при оновленні Excel: {write_result.get('error', 'Невідома помилка')}"
            
            return results
            
        except Exception as e:
            error_msg = f"Помилка при повному процесі обробки {audio_path}: {str(e)}"
            logging.error(error_msg)
            results['error'] = error_msg
            return results
    
    def batch_process_audio_files(self, audio_files: List[str], excel_file_path: str,
                                 output_dir: str = "batch_output") -> Dict[str, Any]:
        """
        Пакетна обробка кількох аудіо файлів
        
        Args:
            audio_files (List[str]): Список шляхів до аудіо файлів
            excel_file_path (str): Шлях до Excel файлу з структурою
            output_dir (str): Директорія для збереження результатів
            
        Returns:
            Dict[str, Any]: Результати обробки всіх файлів
        """
        batch_results = {
            'total_files': len(audio_files),
            'successful': 0,
            'failed': 0,
            'results': [],
            'updated_excel_file': None
        }
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Використовуємо оригінальний Excel файл (згідно з вимогами)
            batch_excel_path = excel_file_path
            
            logging.info(f"Початок пакетної обробки {len(audio_files)} файлів")
            
            for i, audio_file in enumerate(audio_files, 1):
                logging.info(f"Обробка файлу {i}/{len(audio_files)}: {audio_file}")
                
                try:
                    # Обробляємо файл і додаємо новий рядок до оригінального Excel
                    result = self.process_and_update_excel(
                        audio_path=audio_file,
                        excel_file_path=batch_excel_path,
                        target_row=None,  # Завжди знаходимо наступний порожній рядок
                        output_excel_path=None,  # Записуємо в оригінальний файл
                        output_dir=os.path.join(output_dir, f"file_{i:03d}")
                    )
                    
                    if result['success']:
                        batch_results['successful'] += 1
                        logging.info(f"✅ Файл {i} оброблено успішно")
                    else:
                        batch_results['failed'] += 1
                        logging.error(f"❌ Помилка при обробці файлу {i}: {result.get('error', 'Невідома помилка')}")
                    
                    batch_results['results'].append(result)
                    
                except Exception as e:
                    batch_results['failed'] += 1
                    error_result = {
                        'audio_file': audio_file,
                        'success': False,
                        'error': str(e)
                    }
                    batch_results['results'].append(error_result)
                    logging.error(f"❌ Критична помилка при обробці файлу {i}: {str(e)}")
            
            batch_results['updated_excel_file'] = excel_file_path
            
            logging.info(f"🎉 Пакетна обробка завершена!")
            logging.info(f"✅ Успішно: {batch_results['successful']}")
            logging.info(f"❌ З помилками: {batch_results['failed']}")
            logging.info(f"📋 Результуючий Excel: {batch_excel_path}")
            
        except Exception as e:
            logging.error(f"Критична помилка при пакетній обробці: {str(e)}")
            batch_results['error'] = str(e)
        
        return batch_results
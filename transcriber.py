import os
import logging
from datetime import datetime
import json
from typing import Dict, List, Any, Optional, Union
import google.generativeai as genai
from google_sheets_handler import GoogleSheetsHandler
from google_drive_handler import GoogleDriveHandler


class AudioTranscriber:
    """Клас для транскрибації аудіо файлів з використанням Gemini API та Google Sheets"""
    
    def __init__(self, api_key, model_name, google_credentials_file=None):
        self.api_key = api_key
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name=model_name)
        logging.info("Ініціалізовано Gemini API")
        
        # Ініціалізація Google Sheets та Drive handlers
        self.google_credentials_file = google_credentials_file
        self.sheets_handler = None
        self.drive_handler = None
        
        if google_credentials_file and os.path.exists(google_credentials_file):
            try:
                self.sheets_handler = GoogleSheetsHandler(google_credentials_file)
                self.drive_handler = GoogleDriveHandler(google_credentials_file)
                logging.info("Ініціалізовано Google Sheets та Drive API")
            except Exception as e:
                logging.warning(f"Не вдалося ініціалізувати Google API: {e}")
        else:
            logging.info("Google credentials не надано, працюємо без Google інтеграції")
    
    def transcribe_audio(self, audio_path):
        """Транскрибація аудіо файлу"""
        try:
            logging.info(f"Завантаження аудіо файлу: {audio_path}")
            
            with open(audio_path, 'rb') as f:
                audio_bytes = f.read()
            
            import mimetypes
            mime_type = mimetypes.guess_type(audio_path)[0] or 'audio/mpeg'
            
            logging.info(f"Розмір файлу: {len(audio_bytes) / 1024 / 1024:.2f} MB, MIME: {mime_type}")
            
            prompt = """
            Будь ласка, транскрибуй цей аудіо файл українською мовою. 
            Якщо в аудіо звучить інша мова, транскрибуй її оригінальною мовою.
            Збережи структуру мовлення, розділи на абзаци де це доречно.
            """
            
            audio_part = {
                "inline_data": {
                    "mime_type": mime_type,
                    "data": audio_bytes
                }
            }
            
            logging.info("Початок генерації транскрипту (inline mode)...")
            
            response = self.model.generate_content(
                [prompt, audio_part],
                request_options={"timeout": 600}
            )
            
            if response and response.text:
                logging.info(f"Успішно транскрибовано: {audio_path}")
                return response.text
            else:
                logging.error(f"Не вдалося отримати текст для файлу: {audio_path}")
                return None
                
        except Exception as e:
            logging.error(f"Помилка при транскрибації {audio_path}: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def save_transcript(self, text, output_path):
        """Збереження транскрибованого тексту"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"# Транскрипт\n")
                f.write(f"Дата створення: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("---\n\n")
                f.write(text)
            
            logging.info(f"Збережено транскрипт: {output_path}")
            
        except Exception as e:
            logging.error(f"Помилка при збереженні файлу {output_path}: {str(e)}")
    
    def fill_sheets_data(self, transcript_text: str, spreadsheet_id: str, worksheet_name: str = None) -> Dict[str, Any]:
        """Заповнює дані на основі транскрипту, читаючи структуру полів з Google Sheets"""
        try:
            logging.info("📊 Читання структури полів з Google Sheets...")
            fields_structure = self.sheets_handler.read_data(
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name,
                header_row=2,
                data_row=3
            )
            
            if not fields_structure:
                logging.error("Не вдалося прочитати структуру полів з таблиці")
                return {}
            
            logging.info(f"Почато заповнення {len(fields_structure)} полів")
            logging.info(f"🤖 Аналіз всіх полів одним запитом до Gemini...")
            all_results = self._analyze_all_fields_at_once(transcript_text, fields_structure)
            
            filled_data = {}
            for field_name, value in all_results.items():
                filled_data[field_name] = value
            
            logging.info(f"✅ Заповнення завершено: {len(filled_data)} полів")
            return filled_data
            
        except Exception as e:
            logging.error(f"Помилка при заповненні даних: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return {}
    
    def _analyze_all_fields_at_once(self, text: str, fields_structure: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """Аналізує транскрипт для всіх полів одним запитом"""
        try:
            prompt_parts = [
                "Проаналізуй наступний транскрипт телефонної розмови та заповни всі поля.",
                f"\n📞 ТРАНСКРИПТ:\n{text}\n",
                "\n📋 ПОЛЯ ДЛЯ ЗАПОВНЕННЯ:"
            ]
            
            field_mappings = {}
            for i, (field_name, field_info) in enumerate(fields_structure.items(), 1):
                field_type = field_info.get('type', 'text')
                dropdown_options = field_info.get('dropdown_options', [])
                
                if field_type == 'dropdown' and dropdown_options:
                    options_str = ", ".join(str(opt) for opt in dropdown_options)
                    prompt_parts.append(f"{i}. {field_name} (вибір з: {options_str})")
                else:
                    prompt_parts.append(f"{i}. {field_name} (текст)")
                
                field_mappings[i] = field_name
            
            prompt_parts.extend([
                "\n⚠️ ВАЖЛИВО:",
                "- Для dropdown полів обери ТІЛЬКИ одне значення зі списку ТОЧНО ЯК ВОНО НАПИСАНО",
                "- Для числових полів (0/1) пиши тільки цифру БЕЗ пробілів: '0' або '1'",
                "- Для текстових полів дай коротку відповідь (1-2 речення)",
                "- Якщо інформації немає, для dropdown використай перше значення зі списку, для тексту напиши 'Не вказано'",
                "- Відповідь у форматі: '1:значення,2:значення,3:значення' (БЕЗ пробілів після номера та двокрапки)",
                "- НЕ використовуй квадратні дужки [], лапки або зайві пробіли",
                "- Приклад правильної відповіді: '1:Запис на ТО,2:0,3:1,4:Богдан'",
                "\n📝 ВІДПОВІДЬ:"
            ])
            
            full_prompt = "\n".join(prompt_parts)
            response = self.model.generate_content(full_prompt)
            result_text = response.text.strip()
            
            logging.info(f"Отримано відповідь від Gemini для {len(fields_structure)} полів")
            
            results = {}
            parts = result_text.split(',')
            
            for part in parts:
                part = part.strip()
                if ':' in part:
                    try:
                        field_num_str, value = part.split(':', 1)
                        field_num = int(field_num_str.strip())
                        # Очищаємо значення від квадратних дужок, лапок та пробілів
                        value = value.strip().strip('[]').strip().strip('"').strip("'").strip()
                        
                        if field_num in field_mappings:
                            field_name = field_mappings[field_num]
                            field_info = fields_structure[field_name]
                            
                            if field_info.get('type') == 'dropdown':
                                dropdown_options = [str(opt).strip() for opt in field_info.get('dropdown_options', [])]
                                # Порівнюємо очищені значення
                                value_clean = value.strip()
                                
                                if value_clean in dropdown_options:
                                    results[field_name] = value_clean
                                else:
                                    # Пробуємо знайти схоже значення (без урахування регістру)
                                    found = False
                                    for opt in dropdown_options:
                                        if opt.strip().lower() == value_clean.lower():
                                            results[field_name] = opt.strip()
                                            found = True
                                            break
                                    
                                    if not found:
                                        fallback = dropdown_options[0] if dropdown_options else value_clean
                                        results[field_name] = fallback
                                        logging.warning(f"'{field_name}': значення '{value_clean}' не знайдено в {dropdown_options}, використано '{fallback}'")
                                    else:
                                        logging.info(f"✓ {field_name}: {results[field_name]}")
                            else:
                                results[field_name] = value
                            
                            if field_name not in results or not results.get(field_name):
                                logging.info(f"✓ {field_name}: {value}")
                            
                    except ValueError:
                        continue
            
            for field_name, field_info in fields_structure.items():
                if field_name not in results:
                    if field_info.get('type') == 'dropdown':
                        dropdown_options = field_info.get('dropdown_options', [])
                        results[field_name] = str(dropdown_options[0]) if dropdown_options else "Не вказано"
                    else:
                        results[field_name] = "Не вказано"
                    logging.warning(f"⚠️ {field_name}: не заповнено, використано значення за замовчуванням")
            
            return results
            
        except Exception as e:
            logging.error(f"Помилка аналізу полів: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return {}
    
    def process_and_update_sheets(self, audio_path: str = None, audio_file_id: str = None, 
                                 drive_folder_id: str = None, spreadsheet_id: str = None, 
                                 worksheet_name: str = None, local_transcripts_folder: str = None) -> Dict[str, Any]:
        """
        Повний процес: транскрибація + аналіз + запис у Google Sheets
        
        Args:
            audio_path: Локальний шлях до аудіо файлу (якщо є)
            audio_file_id: ID файлу на Google Drive (альтернатива audio_path)
            drive_folder_id: ID папки Drive (потрібен якщо використовується audio_file_id)
            spreadsheet_id: ID Google Sheets документу
            worksheet_name: Назва аркушу
            local_transcripts_folder: Папка для збереження транскриптів
        """
        try:
            # Визначаємо джерело аудіо
            temp_file = None
            if audio_file_id:
                # Завантажуємо файл з Google Drive
                logging.info(f"Завантаження аудіо файлу з Google Drive (ID: {audio_file_id})")
                
                if not self.drive_handler:
                    return {'status': 'error', 'success': False, 'error': 'Drive handler не ініціалізовано'}
                
                # Отримуємо інформацію про файл
                file_metadata = self.drive_handler.service.files().get(
                    fileId=audio_file_id, 
                    fields='name, mimeType'
                ).execute()
                
                file_name = file_metadata.get('name', 'audio.mp3')
                
                # Завантажуємо файл у тимчасову директорію
                import tempfile
                temp_dir = tempfile.gettempdir()
                temp_file = os.path.join(temp_dir, file_name)
                
                request = self.drive_handler.service.files().get_media(fileId=audio_file_id)
                with open(temp_file, 'wb') as f:
                    from googleapiclient.http import MediaIoBaseDownload
                    import io
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                    fh.seek(0)
                    f.write(fh.read())
                
                audio_path = temp_file
                logging.info(f"Файл завантажено локально: {audio_path}")
            
            elif not audio_path:
                return {'status': 'error', 'success': False, 'error': 'Потрібно вказати audio_path або audio_file_id'}
            
            # Транскрибація
            logging.info(f"Транскрибація аудіо файлу: {audio_path}")
            transcript_text = self.transcribe_audio(audio_path)
            
            # Видаляємо тимчасовий файл якщо був створений
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    logging.info(f"Видалено тимчасовий файл: {temp_file}")
                except Exception as e:
                    logging.warning(f"Не вдалося видалити тимчасовий файл: {e}")
            
            if not transcript_text:
                return {'status': 'error', 'success': False, 'error': 'Помилка транскрибації'}
            
            logging.info("Аналіз транскрипту та заповнення полів...")
            filled_data = self.fill_sheets_data(
                transcript_text=transcript_text,
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name
            )
            
            if not filled_data:
                return {
                    'status': 'error',
                    'success': False,
                    'error': 'Помилка заповнення даних',
                    'transcript': transcript_text
                }
            
            logging.info(f"Запис результатів у Google Sheets")
            filename = os.path.basename(audio_path) if audio_path else file_name
            
            write_result = self.sheets_handler.write_data_to_row(
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name,
                data=filled_data,
                filename=filename,
                transcript=transcript_text
            )
            
            if write_result:
                logging.info(f"✅ Успішно записано дані у Google Sheets")
                return {
                    'status': 'success',
                    'success': True,
                    'transcript': transcript_text,
                    'filled_data': filled_data
                }
            else:
                error_msg = 'Помилка запису у Sheets'
                logging.error(f"❌ {error_msg}")
                return {
                    'status': 'error',
                    'success': False,
                    'error': error_msg,
                    'transcript': transcript_text,
                    'filled_data': filled_data
                }
                
        except Exception as e:
            error_msg = f"Критична помилка при обробці: {str(e)}"
            logging.error(error_msg)
            import traceback
            logging.error(traceback.format_exc())
            
            # Видаляємо тимчасовий файл при помилці
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            
            return {'status': 'error', 'success': False, 'error': error_msg}


if __name__ == "__main__":
    transcriber = AudioTranscriber(model_name="gemini-2.5-flash", api_key="AIzaSyDi-2Xk5BJmK15KuMyWH0djqd5ni0inIyM")
    audio_file = 'google_folder/2025-09-03_10-41_0963363703_incoming.mp3'
    result = transcriber.transcribe_audio(audio_file)
    with open("transcript.txt", "w", encoding="utf-8") as f:
        f.write(result)

"""
Google Sheets Handler Module

Модуль для роботи з Google Sheets - читання та запис даних.
Замінює функціонал excel_reader.py та excel_writer.py для роботи з Google таблицями.

Usage:
    from google_sheets_handler import GoogleSheetsHandler
    
    handler = GoogleSheetsHandler('credentials.json')
    data = handler.read_data('spreadsheet_id', 'Sheet1')
"""

import logging
import gspread
from google.oauth2.service_account import Credentials
from typing import Dict, List, Any, Optional, Union
from datetime import datetime


class GoogleSheetsHandler:
    """
    Клас для роботи з Google Sheets API
    
    Attributes:
        credentials_file (str): Шлях до файлу з credentials
        client: Клієнт gspread для роботи з Google Sheets
        verbose (bool): Режим детального виводу інформації
    """
    
    # Область доступу для Google Sheets та Google Drive
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(self, credentials_file: str, verbose: bool = False):
        """
        Ініціалізує GoogleSheetsHandler
        
        Args:
            credentials_file (str): Шлях до JSON файлу з service account credentials
            verbose (bool): Увімкнути детальний вивід інформації
            
        Raises:
            FileNotFoundError: Якщо credentials файл не знайдено
            Exception: Якщо не вдається авторизуватися
        """
        self.credentials_file = credentials_file
        self.verbose = verbose
        self.client = None
        self.sheets_service = None  # Додаємо Sheets API v4 service
        
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Авторизація в Google Sheets API"""
        try:
            credentials = Credentials.from_service_account_file(
                self.credentials_file,
                scopes=self.SCOPES
            )
            self.client = gspread.authorize(credentials)
            
            # Ініціалізуємо Sheets API v4 для додаткових операцій
            from googleapiclient.discovery import build
            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            
            if self.verbose:
                logging.info(f"Успішно авторизовано в Google Sheets API")
        except FileNotFoundError:
            raise FileNotFoundError(f"Credentials файл не знайдено: {self.credentials_file}")
        except Exception as e:
            raise Exception(f"Помилка авторизації Google Sheets: {e}")
    
    def _read_dropdown_options(self, worksheet, row: int) -> Dict[int, List[str]]:
        """
        Читає dropdown опції з вказаного рядка через API
        
        Args:
            worksheet: Об'єкт worksheet
            row (int): Номер рядка для читання dropdown (зазвичай 3)
            
        Returns:
            Dict[int, List[str]]: Словник {номер_стовпця: [опції]}
        """
        try:
            spreadsheet_id = worksheet.spreadsheet.id
            sheet_id = worksheet.id
            
            # Запитуємо метадані таблиці через Sheets API v4
            result = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[f'{worksheet.title}!{row}:{row}'],
                fields='sheets(data(rowData(values(dataValidation))))'
            ).execute()
            
            dropdown_map = {}
            
            # Парсимо відповідь
            sheets = result.get('sheets', [])
            if sheets:
                data = sheets[0].get('data', [])
                if data:
                    row_data = data[0].get('rowData', [])
                    if row_data:
                        values = row_data[0].get('values', [])
                        for col_idx, cell in enumerate(values):
                            validation = cell.get('dataValidation')
                            if validation:
                                condition = validation.get('condition', {})
                                if condition.get('type') == 'ONE_OF_LIST':
                                    # Dropdown з списком
                                    options = condition.get('values', [])
                                    dropdown_list = [opt.get('userEnteredValue', '') for opt in options]
                                    dropdown_map[col_idx + 1] = dropdown_list  # 1-based
                                elif condition.get('type') == 'ONE_OF_RANGE':
                                    # Dropdown з діапазону (складніше, поки пропускаємо)
                                    pass
            
            if self.verbose and dropdown_map:
                logging.info(f"Знайдено {len(dropdown_map)} dropdown полів у рядку {row}")
            
            return dropdown_map
            
        except Exception as e:
            logging.warning(f"Не вдалося прочитати dropdown опції: {e}")
            return {}
    
    def get_spreadsheet(self, spreadsheet_id: str):
        """
        Отримує spreadsheet за ID
        
        Args:
            spreadsheet_id (str): ID Google Sheets документу
            
        Returns:
            Spreadsheet: Об'єкт spreadsheet
        """
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            if self.verbose:
                logging.info(f"Відкрито таблицю: {spreadsheet.title}")
            return spreadsheet
        except Exception as e:
            logging.error(f"Помилка відкриття таблиці {spreadsheet_id}: {e}")
            logging.error(f"Переконайтесь що Service Account має доступ до таблиці!")
            logging.error(f"Email для доступу: {self.credentials_file}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    def get_worksheet(self, spreadsheet_id: str, worksheet_name: str = None, worksheet_index: int = 0):
        """
        Отримує worksheet (аркуш) з spreadsheet
        
        Args:
            spreadsheet_id (str): ID Google Sheets документу
            worksheet_name (str): Назва аркушу (опційно)
            worksheet_index (int): Індекс аркушу якщо назва не вказана
            
        Returns:
            Worksheet: Об'єкт worksheet
        """
        try:
            spreadsheet = self.get_spreadsheet(spreadsheet_id)
            
            if worksheet_name:
                worksheet = spreadsheet.worksheet(worksheet_name)
            else:
                worksheet = spreadsheet.get_worksheet(worksheet_index)
            
            if self.verbose:
                logging.info(f"Відкрито аркуш: {worksheet.title}")
            
            return worksheet
        except Exception as e:
            logging.error(f"Помилка відкриття аркушу: {e}")
            raise
    
    def get_dropdown_values(self, worksheet, row: int, col: int) -> Optional[List[str]]:
        """
        Отримує значення випадаючого списку для комірки
        (Google Sheets API має обмеження щодо отримання data validation,
        тому ця функція може повертати None)
        
        Args:
            worksheet: Об'єкт worksheet
            row (int): Номер рядка (1-based)
            col (int): Номер стовпця (1-based)
        
        Returns:
            Optional[List[str]]: Список значень або None
        """
        try:
            # Google Sheets API v4 не підтримує прямий доступ до data validation через gspread
            # Можна використовувати spreadsheets().get() з полем 'dataValidation'
            # Але це вимагає додаткового API запиту
            
            # Для простоти повертаємо None, якщо потрібна ця функція,
            # можна розширити через google-api-python-client
            if self.verbose:
                logging.warning("Отримання dropdown values потребує додаткової імплементації через API v4")
            return None
            
        except Exception as e:
            logging.error(f"Помилка отримання dropdown values: {e}")
            return None
    
    def read_data(self, spreadsheet_id: str, worksheet_name: str = None, 
                  header_row: int = 2, data_row: int = 3) -> Dict[str, Dict[str, Any]]:
        """
        Читає структуру даних з Google Sheets
        Рядок 2: заголовки полів
        Рядок 3: приклади даних (використовується для визначення dropdown опцій)
        
        Args:
            spreadsheet_id (str): ID Google Sheets документу
            worksheet_name (str): Назва аркушу
            header_row (int): Номер рядка з заголовками (за замовчуванням 2)
            data_row (int): Номер рядка з прикладами даних (за замовчуванням 3)
            
        Returns:
            Dict[str, Dict[str, Any]]: Словник з структурою полів
        """
        try:
            worksheet = self.get_worksheet(spreadsheet_id, worksheet_name)
            
            # Отримуємо всі дані з аркушу
            all_values = worksheet.get_all_values()
            
            if len(all_values) < max(header_row, data_row):
                logging.warning("Недостатньо рядків у таблиці")
                return {}
            
            # Отримуємо заголовки з рядка 2
            headers = all_values[header_row - 1]
            
            # Отримуємо приклади даних з рядка 3 (для dropdown)
            data_values = all_values[data_row - 1] if len(all_values) >= data_row else []
            
            # Читаємо dropdown опції з рядка 3 через API
            dropdown_options_map = self._read_dropdown_options(worksheet, data_row)
            
            # Формуємо структуру даних
            result = {}
            for col_idx, header in enumerate(headers):
                if header and header.strip():
                    field_name = header.strip()
                    description = data_values[col_idx] if col_idx < len(data_values) else ""
                    col_num = col_idx + 1  # 1-based
                    
                    # Перевіряємо чи є dropdown для цього стовпця
                    dropdown_opts = dropdown_options_map.get(col_num)
                    field_type = 'dropdown' if dropdown_opts else 'text'
                    
                    result[field_name] = {
                        'column': col_num,
                        'description': description,
                        'type': field_type,
                        'dropdown_options': dropdown_opts
                    }
            
            if self.verbose:
                logging.info(f"Прочитано {len(result)} полів з таблиці")
                dropdown_count = sum(1 for f in result.values() if f['type'] == 'dropdown')
                logging.info(f"Знайдено {dropdown_count} dropdown полів")
            
            return result
            
        except Exception as e:
            logging.error(f"Помилка читання даних з Google Sheets: {e}")
            raise
    
    def _copy_entire_row(self, worksheet, source_row: int, target_row: int) -> bool:
        """
        Копіює весь рядок (форматування, умовне форматування, dropdown, але БЕЗ значень)
        
        Args:
            worksheet: Worksheet об'єкт gspread
            source_row: Номер рядка-джерела (наприклад, 3)
            target_row: Номер цільового рядка
            
        Returns:
            True якщо успішно
        """
        try:
            spreadsheet_id = worksheet.spreadsheet.id
            sheet_id = worksheet.id
            
            # Використовуємо Sheets API для копіювання форматування, валідації та умовного форматування
            request_body = {
                'requests': [
                    # Крок 1: Копіюємо форматування (кольори, шрифти, вирівнювання, обрамлення)
                    {
                        'copyPaste': {
                            'source': {
                                'sheetId': sheet_id,
                                'startRowIndex': source_row - 1,
                                'endRowIndex': source_row,
                            },
                            'destination': {
                                'sheetId': sheet_id,
                                'startRowIndex': target_row - 1,
                                'endRowIndex': target_row,
                            },
                            'pasteType': 'PASTE_FORMAT',  # Форматування
                        }
                    },
                    # Крок 2: Копіюємо Data Validation (dropdown списки)
                    {
                        'copyPaste': {
                            'source': {
                                'sheetId': sheet_id,
                                'startRowIndex': source_row - 1,
                                'endRowIndex': source_row,
                            },
                            'destination': {
                                'sheetId': sheet_id,
                                'startRowIndex': target_row - 1,
                                'endRowIndex': target_row,
                            },
                            'pasteType': 'PASTE_DATA_VALIDATION',  # Валідація даних (dropdown)
                        }
                    },
                    # Крок 3: Копіюємо умовне форматування (кольори в залежності від значення)
                    {
                        'copyPaste': {
                            'source': {
                                'sheetId': sheet_id,
                                'startRowIndex': source_row - 1,
                                'endRowIndex': source_row,
                            },
                            'destination': {
                                'sheetId': sheet_id,
                                'startRowIndex': target_row - 1,
                                'endRowIndex': target_row,
                            },
                            'pasteType': 'PASTE_CONDITIONAL_FORMATTING',  # Умовне форматування
                        }
                    },
                    # Крок 4: Очищаємо значення в цільовому рядку (залишаємо тільки форматування)
                    {
                        'updateCells': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': target_row - 1,
                                'endRowIndex': target_row,
                            },
                            'fields': 'userEnteredValue',
                        }
                    }
                ]
            }
            
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
            
            logging.info(f"✅ Скопійовано форматування, валідацію даних та умовне форматування: рядок {source_row} → {target_row}")
            return True
            
        except Exception as e:
            logging.error(f"Помилка копіювання рядка: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return False
    
    def find_next_empty_row(self, worksheet, start_row: int = 4) -> int:
        """
        Знаходить наступний порожній рядок у таблиці
        
        Args:
            worksheet: Об'єкт worksheet
            start_row (int): Рядок з якого починати пошук
            
        Returns:
            int: Номер порожнього рядка
        """
        try:
            # Отримуємо всі значення з таблиці
            all_values = worksheet.get_all_values()
            
            # Шукаємо перший порожній рядок після start_row
            for row_idx in range(start_row - 1, len(all_values)):
                row = all_values[row_idx]
                # Перевіряємо чи всі комірки в рядку порожні (або хоча б перші 5)
                is_empty = all(not cell or cell.strip() == "" for cell in row[:5])
                if is_empty:
                    found_row = row_idx + 1
                    logging.info(f"🔍 Знайдено порожній рядок: {found_row}")
                    return found_row
            
            # Якщо всі рядки заповнені, повертаємо наступний після останнього
            next_row = len(all_values) + 1
            logging.info(f"🔍 Всі рядки заповнені, додаємо новий: {next_row}")
            return next_row
            
        except Exception as e:
            logging.error(f"Помилка пошуку порожнього рядка: {e}")
            # У випадку помилки, отримуємо загальну кількість рядків і додаємо 1
            try:
                row_count = worksheet.row_count
                return row_count + 1
            except:
                return start_row
    
    def _copy_row_formatting(self, worksheet, source_row: int, target_row: int):
        """
        Копіює форматування (колір, dropdown, borders тощо) з одного рядка в інший
        
        Args:
            worksheet: Об'єкт worksheet
            source_row (int): Рядок-джерело форматування
            target_row (int): Рядок-призначення
        """
        try:
            # Використовуємо Google Sheets API для копіювання форматування
            spreadsheet_id = worksheet.spreadsheet.id
            sheet_id = worksheet.id
            
            # Формуємо запит на копіювання форматування
            requests = [{
                'copyPaste': {
                    'source': {
                        'sheetId': sheet_id,
                        'startRowIndex': source_row - 1,  # 0-based
                        'endRowIndex': source_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 100  # Копіюємо перші 100 стовпців
                    },
                    'destination': {
                        'sheetId': sheet_id,
                        'startRowIndex': target_row - 1,  # 0-based
                        'endRowIndex': target_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 100
                    },
                    'pasteType': 'PASTE_FORMAT',  # Тільки форматування (dropdown, кольори, borders)
                }
            }]
            
            body = {'requests': requests}
            
            # Виконуємо запит через API
            worksheet.spreadsheet.batch_update(body)
            
            if self.verbose:
                logging.info(f"Скопійовано форматування з рядка {source_row} на рядок {target_row}")
                
        except Exception as e:
            logging.error(f"Помилка копіювання форматування: {e}")
            # Не критична помилка, продовжуємо без форматування
    
    def write_data_to_row(self, spreadsheet_id: str, worksheet_name: str, 
                         data: Dict[str, str], target_row: int = None,
                         header_row: int = 2, filename: str = None,
                         transcript: str = None) -> bool:
        """
        Записує дані у вказаний рядок Google Sheets зі збереженням форматування
        Спочатку копіює рядок 3 (шаблон) → target_row, потім заповнює значення
        
        Args:
            spreadsheet_id (str): ID Google Sheets документу
            worksheet_name (str): Назва аркушу
            data (Dict[str, str]): Дані для запису {назва_поля: значення}
            target_row (int): Номер рядка для запису (якщо None - знаходить автоматично)
            header_row (int): Номер рядка з заголовками (2)
            filename (str): Назва файлу
            transcript (str): Текст транскрипту
            
        Returns:
            bool: True якщо запис успішний
        """
        try:
            worksheet = self.get_worksheet(spreadsheet_id, worksheet_name)
            
            # Якщо target_row не вказано, знаходимо наступний порожній рядок
            if target_row is None:
                target_row = self.find_next_empty_row(worksheet)
                logging.info(f"Автоматично обрано рядок для запису: {target_row}")
            
            # Рядок 3 - це шаблон з форматуванням та dropdown
            template_row = 3
            
            # КРОК 1: Копіюємо весь рядок 3 → target_row (зі збереженням dropdown)
            logging.info(f"📋 Копіювання рядка {template_row} → рядок {target_row}")
            self._copy_entire_row(worksheet, template_row, target_row)
            
            # КРОК 2: Читаємо заголовки з рядка 2
            headers = worksheet.row_values(header_row)
            
            # Створюємо мапу заголовків до номерів стовпців
            header_map = {}
            for col_idx, header in enumerate(headers):
                if header and header.strip():
                    header_map[header.strip()] = col_idx + 1  # 1-based
            
            # КРОК 3: Заповнюємо дані
            written_fields = 0
            updates = []
            
            # Основні поля
            for field_name, value in data.items():
                if field_name in header_map:
                    col_num = header_map[field_name]
                    cell_address = gspread.utils.rowcol_to_a1(target_row, col_num)
                    updates.append({
                        'range': cell_address,
                        'values': [[str(value)]]
                    })
                    written_fields += 1
            
            # Назва файлу
            if filename:
                for field_name in ["Назва файлу", "Файл", "Filename"]:
                    if field_name in header_map:
                        col_num = header_map[field_name]
                        cell_address = gspread.utils.rowcol_to_a1(target_row, col_num)
                        updates.append({
                            'range': cell_address,
                            'values': [[filename]]
                        })
                        written_fields += 1
                        break
            
            # Транскрипт
            if transcript:
                for field_name in ["Транскрипт", "Транскрипція", "Текст", "Transcript"]:
                    if field_name in header_map:
                        col_num = header_map[field_name]
                        cell_address = gspread.utils.rowcol_to_a1(target_row, col_num)
                        transcript_text = transcript[:50000] if len(transcript) > 50000 else transcript
                        updates.append({
                            'range': cell_address,
                            'values': [[transcript_text]]
                        })
                        written_fields += 1
                        logging.info(f"Додано транскрипт ({len(transcript_text)} символів)")
                        break
            
            # КРОК 4: Виконуємо batch update для звичайних значень
            if updates:
                worksheet.batch_update(updates, value_input_option='RAW')
                logging.info(f"✅ Записано {written_fields} полів у рядок {target_row}")
            
            # КРОК 5: Додаємо формулу для поля "Оцінка" окремим запитом (з USER_ENTERED)
            for field_name in ["Оцінка", "Загальний бал", "Сума", "Total"]:
                if field_name in header_map:
                    col_num = header_map[field_name]
                    # Формула: =F{row}+G{row}+H{row}+I{row}+J{row}+K{row}+M{row}+O{row}
                    formula = f"=F{target_row}+G{target_row}+H{target_row}+I{target_row}+J{target_row}+K{target_row}+M{target_row}+O{target_row}"
                    cell_address = gspread.utils.rowcol_to_a1(target_row, col_num)
                    
                    # Записуємо формулу з USER_ENTERED щоб вона обчислювалась
                    worksheet.update(cell_address, [[formula]], value_input_option='USER_ENTERED')
                    logging.info(f"✅ Додано формулу в поле '{field_name}': {formula}")
                    break
            
            return True
            
        except Exception as e:
            logging.error(f"Помилка запису даних в Google Sheets: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return False
    
    def __enter__(self):
        """Context manager enter"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        # Google Sheets API не потребує явного закриття з'єднання
        pass

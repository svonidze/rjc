#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to verify text presence from column G on web pages using links from column I
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import time
import argparse
from datetime import datetime
from urllib.parse import urlparse
import sys
import os

# Настройка логирования
log_filename = f'check_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Заголовки для имитации браузера
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}

def check_text_on_page(url, text_to_find, timeout=10):
    """
    Проверяет наличие текста на веб-странице
    
    Args:
        url: URL страницы
        text_to_find: Текст для поиска
        timeout: Таймаут запроса в секундах
    
    Returns:
        tuple: (найден ли текст, сообщение об ошибке если есть)
    """
    try:
        # Проверка валидности URL
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False, "Invalid URL"
        
        # Выполнение запроса
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        # Определение кодировки
        response.encoding = response.apparent_encoding
        
        # Парсинг HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Получение текста страницы
        page_text = soup.get_text(separator=' ', strip=True)
        
        # Проверка наличия искомого текста
        text_found = text_to_find.strip() in page_text
        
        return text_found, None
        
    except requests.exceptions.Timeout:
        return False, f"Timeout when accessing {url}"
    except requests.exceptions.ConnectionError:
        return False, f"Connection error to {url}"
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP error {e.response.status_code} for {url}"
    except requests.exceptions.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def process_excel_file(filename, delay=1, sheet_names=None):
    """
    Обрабатывает Excel файл и проверяет ссылки

    Args:
        filename: Путь к Excel файлу
        delay: Задержка между запросами в секундах
        sheet_names: Имена или индексы листов для обработки (None - все листы)
    """
    logger.info(f"Starting processing file: {filename}")

    try:
        # Чтение Excel файла
        logger.info("Reading Excel file...")

        # Чтение листов
        if sheet_names is None:
            # Читаем все листы
            sheets_dict = pd.read_excel(filename, sheet_name=None)
            logger.info(f"Found sheets: {len(sheets_dict)}")
        else:
            # Читаем указанные листы
            sheets_dict = pd.read_excel(filename, sheet_name=sheet_names)
            if isinstance(sheets_dict, dict):
                logger.info(f"Selected sheets: {len(sheets_dict)}")
            else:
                # Если указан один лист, pandas возвращает DataFrame, а не dict
                sheets_dict = {sheet_names[0] if isinstance(sheet_names, list) else sheet_names: sheets_dict}
                logger.info("Selected one sheet")

        total_rows_all = 0
        found_count_all = 0
        not_found_count_all = 0
        error_count_all = 0

        # Обработка каждого листа
        for sheet_name, df in sheets_dict.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing sheet: '{sheet_name}'")
            logger.info(f"{'='*60}")

            # Проверка наличия необходимых столбцов
            if 'G' not in df.columns and len(df.columns) < 7:
                logger.error(f"Sheet '{sheet_name}': Column G not found - skipped")
                continue

            if 'I' not in df.columns and len(df.columns) < 9:
                logger.error(f"Sheet '{sheet_name}': Column I not found - skipped")
                continue

            # Получение столбцов по индексу (G=6, I=8 в нулевой индексации)
            text_column = df.iloc[:, 6]  # Столбец G
            link_column = df.iloc[:, 8]  # Столбец I

            logger.info(f"Sheet '{sheet_name}': Found rows to process: {len(df)}")

            total_rows = len(df)
            found_count = 0
            not_found_count = 0
            error_count = 0

            # Обработка каждой строки в листе
            for idx, (text, link) in enumerate(zip(text_column, link_column), start=1):
                logger.info(f"\nProcessing row {idx}/{total_rows} (sheet '{sheet_name}')")

                # Проверка на пустые значения
                if pd.isna(text) or pd.isna(link):
                    logger.warning(f"Row {idx} (sheet '{sheet_name}'): Empty text or link - skipped")
                    error_count += 1
                    continue

                text = str(text).strip()
                link = str(link).strip()

                logger.info(f"Text to search: {text[:100]}{'...' if len(text) > 100 else ''}")
                logger.info(f"Link: {link}")

                # Проверка наличия текста на странице
                found, error = check_text_on_page(link, text)

                if error:
                    logger.error(f"Row {idx} (sheet '{sheet_name}'): ERROR - {error}")
                    error_count += 1
                elif found:
                    logger.info(f"Row {idx} (sheet '{sheet_name}'): ✓ FOUND - Text present on page")
                    found_count += 1
                else:
                    logger.warning(f"Row {idx} (sheet '{sheet_name}'): ✗ NOT FOUND - Text absent from page")
                    not_found_count += 1

                # Задержка между запросами
                if idx < total_rows:
                    time.sleep(delay)

            # Статистика по листу
            logger.info(f"\n{'-'*60}")
            logger.info(f"SHEET '{sheet_name}' STATISTICS:")
            logger.info(f"Total rows processed: {total_rows}")
            logger.info(f"Text found: {found_count}")
            logger.info(f"Text not found: {not_found_count}")
            logger.info(f"Processing errors: {error_count}")

            # Накопление общей статистики
            total_rows_all += total_rows
            found_count_all += found_count
            not_found_count_all += not_found_count
            error_count_all += error_count

        # Итоговая статистика по всем листам
        logger.info(f"\n{'='*80}")
        logger.info("FINAL STATISTICS FOR ALL SHEETS:")
        logger.info(f"Total rows processed: {total_rows_all}")
        logger.info(f"Text found: {found_count_all}")
        logger.info(f"Text not found: {not_found_count_all}")
        logger.info(f"Processing errors: {error_count_all}")
        logger.info(f"Results saved to file: {log_filename}")

    except FileNotFoundError:
        logger.error(f"File {filename} not found")
    except Exception as e:
        logger.error(f"Critical error processing file: {str(e)}")
        raise

def main():
    """Главная функция"""
    # Настройка парсера аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Check text from column G on web pages using links from column I',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python check_links.py data.xlsx  # Process first sheet only
  python check_links.py data.xlsx --all-sheets  # Process all sheets
  python check_links.py data.xlsx --sheets 0 2  # Process sheets by index
  python check_links.py data.xlsx --sheets "Sheet1" "Sheet3"  # Process sheets by name
  python check_links.py data.xlsx --delay 2 --timeout 15  # With custom settings
        """
    )
    
    parser.add_argument(
        'excel_file',
        type=str,
        help='Path to Excel file to process'
    )

    parser.add_argument(
        '-d', '--delay',
        type=float,
        default=1.0,
        help='Delay between requests in seconds (default: 1.0)'
    )

    parser.add_argument(
        '-t', '--timeout',
        type=int,
        default=10,
        help='Timeout for HTTP requests in seconds (default: 10)'
    )

    sheet_group = parser.add_mutually_exclusive_group()

    sheet_group.add_argument(
        '--all-sheets',
        action='store_true',
        help='Process all sheets in the Excel file'
    )

    sheet_group.add_argument(
        '--sheets',
        type=str,
        nargs='+',
        help='Specify sheet names or indices to process (default: first sheet only)'
    )
    
    # Парсинг аргументов
    args = parser.parse_args()
    
    # Проверка существования файла
    if not os.path.exists(args.excel_file):
        print(f"Error: File '{args.excel_file}' not found!")
        sys.exit(1)

    # Определение листов для обработки
    sheet_names = 0  # По умолчанию первый лист (индекс 0)
    if args.all_sheets:
        sheet_names = None  # Все листы
    elif args.sheets:
        # Попытка преобразовать строки в числа (индексы) или оставить как имена
        sheet_names = []
        for sheet in args.sheets:
            try:
                # Если это число, преобразуем в int
                sheet_names.append(int(sheet))
            except ValueError:
                # Если не число, оставляем как строку (имя листа)
                sheet_names.append(sheet)

    # Определение текста о выбранных листах
    if args.all_sheets:
        sheets_info = "All sheets"
    elif args.sheets:
        sheets_info = f"Sheets: {', '.join(str(s) for s in args.sheets)}"
    else:
        sheets_info = "First sheet"

    print("="*80)
    print("Web page text verification script")
    print("="*80)
    print(f"Excel file: {args.excel_file}")
    print(f"Sheets to process: {sheets_info}")
    print(f"Log file: {log_filename}")
    print(f"Delay between requests: {args.delay} sec")
    print(f"Request timeout: {args.timeout} sec")
    print("="*80)
    print()

    # Запуск обработки
    process_excel_file(args.excel_file, delay=args.delay, sheet_names=sheet_names)

    print()
    print("="*80)
    print("Processing completed!")
    print(f"Detailed results in file: {log_filename}")
    print("="*80)

if __name__ == "__main__":
    main()


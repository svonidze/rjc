#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки наличия текста из столбца G на веб-страницах по ссылкам из столбца I
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
            return False, "Невалидный URL"
        
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
        return False, f"Таймаут при обращении к {url}"
    except requests.exceptions.ConnectionError:
        return False, f"Ошибка соединения с {url}"
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP ошибка {e.response.status_code} для {url}"
    except requests.exceptions.RequestException as e:
        return False, f"Ошибка запроса: {str(e)}"
    except Exception as e:
        return False, f"Неожиданная ошибка: {str(e)}"

def process_excel_file(filename, delay=1):
    """
    Обрабатывает Excel файл и проверяет ссылки
    
    Args:
        filename: Путь к Excel файлу
        delay: Задержка между запросами в секундах
    """
    logger.info(f"Начало обработки файла: {filename}")
    
    try:
        # Чтение Excel файла
        logger.info("Чтение Excel файла...")
        df = pd.read_excel(filename)
        
        # Проверка наличия необходимых столбцов
        if 'G' not in df.columns and len(df.columns) < 7:
            logger.error("Столбец G не найден в файле")
            return
        
        if 'I' not in df.columns and len(df.columns) < 9:
            logger.error("Столбец I не найден в файле")
            return
        
        # Получение столбцов по индексу (G=6, I=8 в нулевой индексации)
        text_column = df.iloc[:, 6]  # Столбец G
        link_column = df.iloc[:, 8]  # Столбец I
        
        logger.info(f"Найдено строк для обработки: {len(df)}")
        
        total_rows = len(df)
        found_count = 0
        not_found_count = 0
        error_count = 0
        
        # Обработка каждой строки
        for idx, (text, link) in enumerate(zip(text_column, link_column), start=1):
            logger.info(f"\n{'='*80}")
            logger.info(f"Обработка строки {idx}/{total_rows}")
            
            # Проверка на пустые значения
            if pd.isna(text) or pd.isna(link):
                logger.warning(f"Строка {idx}: Пустой текст или ссылка - пропущена")
                error_count += 1
                continue
            
            text = str(text).strip()
            link = str(link).strip()
            
            logger.info(f"Текст для поиска: {text[:100]}{'...' if len(text) > 100 else ''}")
            logger.info(f"Ссылка: {link}")
            
            # Проверка наличия текста на странице
            found, error = check_text_on_page(link, text)
            
            if error:
                logger.error(f"Строка {idx}: ОШИБКА - {error}")
                error_count += 1
            elif found:
                logger.info(f"Строка {idx}: ✓ НАЙДЕНО - Текст присутствует на странице")
                found_count += 1
            else:
                logger.warning(f"Строка {idx}: ✗ НЕ НАЙДЕНО - Текст отсутствует на странице")
                not_found_count += 1
            
            # Задержка между запросами
            if idx < total_rows:
                time.sleep(delay)
        
        # Итоговая статистика
        logger.info(f"\n{'='*80}")
        logger.info("ИТОГОВАЯ СТАТИСТИКА:")
        logger.info(f"Всего обработано строк: {total_rows}")
        logger.info(f"Текст найден: {found_count}")
        logger.info(f"Текст не найден: {not_found_count}")
        logger.info(f"Ошибки при обработке: {error_count}")
        logger.info(f"Результаты сохранены в файл: {log_filename}")
        
    except FileNotFoundError:
        logger.error(f"Файл {filename} не найден")
    except Exception as e:
        logger.error(f"Критическая ошибка при обработке файла: {str(e)}")
        raise

def main():
    """Главная функция"""
    # Настройка парсера аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Check text from column G on web pages using links from column I',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python check_links.py data.xlsx
  python check_links.py data.xlsx --delay 2
  python check_links.py data.xlsx --delay 0.5 --timeout 15
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
    
    # Парсинг аргументов
    args = parser.parse_args()
    
    # Проверка существования файла
    if not os.path.exists(args.excel_file):
        print(f"Ошибка: Файл '{args.excel_file}' не найден!")
        sys.exit(1)
    
    print("="*80)
    print("Скрипт проверки наличия текста на веб-страницах")
    print("="*80)
    print(f"Excel файл: {args.excel_file}")
    print(f"Файл лога: {log_filename}")
    print(f"Задержка между запросами: {args.delay} сек")
    print(f"Таймаут запросов: {args.timeout} сек")
    print("="*80)
    print()
    
    # Запуск обработки
    process_excel_file(args.excel_file, delay=args.delay)
    
    print()
    print("="*80)
    print("Обработка завершена!")
    print(f"Подробные результаты смотрите в файле: {log_filename}")
    print("="*80)

if __name__ == "__main__":
    main()


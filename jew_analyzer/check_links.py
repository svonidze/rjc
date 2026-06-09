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
import re
import csv
from datetime import datetime
from urllib.parse import urlparse
import sys
import os

# ============================================================================
# КОНСТАНТЫ ДЛЯ НАСТРОЙКИ FUZZY MATCH
# ============================================================================

# Минимальная длина очищенного текста для применения fuzzy поиска (в символах)
MIN_FUZZY_TEXT_LENGTH = 3

# Минимальный процент найденных слов в правильном порядке для fuzzy match (0.0 - 1.0)
# Формула: (количество найденных слов в порядке / количество слов в поисковом тексте) >= MIN_MATCH_RATIO
# 
# ВАЖНО: Fuzzy match ищет слова В ТОМ ЖЕ ПОРЯДКЕ, игнорируя спецсимволы и лишние слова между ними
#
# Примеры:
#   0.7 (70%) - строгий поиск (рекомендуется)
#   0.8 (80%) - очень строгий поиск
#   0.9 (90%) - почти все слова должны быть найдены в порядке
MIN_MATCH_RATIO = 0.7

# Абсолютный минимум найденных слов в порядке (защита от ложных срабатываний)
# Даже если процент высокий, должно быть минимум столько слов в правильном порядке
MIN_WORDS_IN_SEQUENCE = 3

# Максимальное количество "лишних" слов между искомыми словами
# Например, если ищем "слово1 слово2", а на странице "слово1 лишнее1 лишнее2 слово2"
# то между ними 2 лишних слова
MAX_WORDS_BETWEEN = 1

# Количество слов контекста вокруг найденного текста для логирования
CONTEXT_WORDS_BEFORE = 20  # Слов до найденного текста
CONTEXT_WORDS_AFTER = 20   # Слов после найденного текста

# ============================================================================
# НАСТРОЙКИ HTTP ЗАПРОСОВ
# ============================================================================

# Глобальный логгер (будет настроен позже в main)
logger = logging.getLogger(__name__)

def setup_logging(log_dir=None):
    """
    Настраивает логирование с указанным путем к логам

    Args:
        log_dir: Путь к папке для логов (None - использовать 'logs')
    """
    global log_filename

    # Определяем папку для логов
    if log_dir is None:
        log_dir = 'logs'
    else:
        log_dir = log_dir.strip()

    # Создаем папку, если она не существует
    created_dir = False
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        created_dir = True

    # Генерируем имя лог-файла
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f'check_results_{timestamp}.log')

    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # Теперь можем использовать logger
    if created_dir:
        logger.info(f"Created logs directory: {log_dir}")

def clean_text_for_search(text, remove_digits=False):
    """
    Очищает текст от специальных символов для более гибкого поиска
    
    Удаляет:
    - Конструкции [id123|текст] → оставляет только "текст"
    - Спецсимволы и знаки препинания → заменяются на пробелы
    - Опционально: слова с цифрами (если remove_digits=True)

    Args:
        text: Исходный текст
        remove_digits: Если True, удаляет слова содержащие цифры и сами цифры

    Returns:
        str: Очищенный текст
    """
    if not text or not isinstance(text, str):
        return ""

    # Шаг 1: Удаляем конструкции типа [id123|текст] или [club456|название]
    # Оставляем только текст после |, если он есть
    cleaned = re.sub(r'\[(?:id|club)\d+\|([^\]]+)\]', r'\1', text)
    
    # Шаг 2: Удаляем оставшиеся квадратные скобки и их содержимое
    cleaned = re.sub(r'\[[^\]]*\]', ' ', cleaned)
    
    # Шаг 3: Удаляем все спецсимволы, оставляя только буквы, цифры и пробелы
    cleaned = re.sub(r'[^\w\s]', ' ', cleaned)
    
    # Шаг 4: Опционально удаляем цифры и слова с цифрами
    if remove_digits:
        # Сначала запоминаем, какие слова содержали цифры
        words = cleaned.split()
        words_with_digits = set()
        for word in words:
            if re.search(r'\d', word):
                words_with_digits.add(word)
        
        # Удаляем все цифры из текста
        cleaned = re.sub(r'\d+', ' ', cleaned)
        
        # Теперь удаляем фрагменты слов, которые остались от слов с цифрами
        # Например: "id123" → "id" (удаляем), "Текст123" → "Текст" (оставляем)
        words_after = cleaned.split()
        words_filtered = []
        
        for word in words_after:
            # Если это фрагмент слова, которое содержало цифры
            # и длина < 3, то удаляем (это "id", "vk" и т.п.)
            is_fragment = any(orig_word.startswith(word) or orig_word.endswith(word) 
                            for orig_word in words_with_digits)
            
            if is_fragment and len(word) <= 2:
                continue  # Пропускаем короткие фрагменты
            else:
                words_filtered.append(word)
        
        cleaned = ' '.join(words_filtered)
    
    # Шаг 5: Заменяем множественные пробелы на один
    cleaned = re.sub(r'\s+', ' ', cleaned)

    # Убираем пробелы в начале и конце
    return cleaned.strip()

def save_found_matches_to_csv(matches, csv_filename):
    """
    Сохраняет найденные совпадения в CSV файл

    Args:
        matches: Список кортежей (url, text)
        csv_filename: Имя CSV файла для сохранения
    """
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL, escapechar='\\')
            # Записываем заголовки
            writer.writerow(['URL', 'Found Text'])
            # Записываем данные
            for url, text in matches:
                writer.writerow([url, text])
        logger.info(f"Found matches saved to CSV file: {csv_filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving matches to CSV: {e}")
        return False

# HTTP заголовки для имитации браузера
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}

def extract_context(text, position, match_length, words_before=CONTEXT_WORDS_BEFORE, words_after=CONTEXT_WORDS_AFTER):
    """
    Извлекает контекст вокруг найденного текста

    Args:
        text: Полный текст
        position: Позиция начала найденного текста
        match_length: Длина найденного текста
        words_before: Количество слов до найденного текста
        words_after: Количество слов после найденного текста

    Returns:
        tuple: (before_text, found_text, after_text)
    """
    # Извлекаем найденный текст
    found_text = text[position:position + match_length]
    
    # Находим начало контекста (words_before слов назад)
    start = position
    words_count = 0
    while start > 0 and words_count < words_before:
        start -= 1
        if start > 0 and text[start].isspace() and not text[start-1].isspace():
            words_count += 1
    
    # Находим конец контекста (words_after слов вперед)
    end = position + match_length
    words_count = 0
    while end < len(text) and words_count < words_after:
        if end < len(text) - 1 and text[end].isspace() and not text[end+1].isspace():
            words_count += 1
        end += 1
    
    before = text[start:position].strip()
    after = text[position + match_length:end].strip()
    
    return before, found_text.strip(), after

def check_text_on_page(url, text_to_find, timeout=10):
    """
    Проверяет наличие текста на веб-странице

    Args:
        url: URL страницы
        text_to_find: Текст для поиска
        timeout: Таймаут запроса в секундах

    Returns:
        tuple: (найден ли текст, сообщение об ошибке если есть, тип совпадения, контекст)
        тип совпадения: 'exact' - точное совпадение, 'fuzzy' - гибкое совпадение, None - не найдено
        контекст: dict с ключами 'before', 'found', 'after' или None
    """
    try:
        # Проверка валидности URL или локального пути
        parsed = urlparse(url)
        is_local_file = False

        if parsed.scheme in ('http', 'https'):
            # Web URL
            response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
        elif parsed.scheme == 'file' or os.path.exists(url):
            # Локальный файл
            is_local_file = True
            if parsed.scheme == 'file':
                file_path = parsed.path
                # Убираем ведущий слэш для Windows путей
                if file_path.startswith('/') and os.name == 'nt':
                    file_path = file_path[1:]
            else:
                # Для Windows путей вида C:/path, urlparse неправильно разбирает scheme
                # Используем оригинальный URL как путь к файлу
                file_path = url

            if not os.path.exists(file_path):
                return False, f"Local file not found: {file_path}", None, None

            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            # Создаем объект, имитирующий requests response
            class MockResponse:
                def __init__(self, text):
                    self.text = text
                    self.status_code = 200

                def raise_for_status(self):
                    pass

            response = MockResponse(content)
        else:
            return False, "Invalid URL or file path", None, None
        
        # Определение кодировки (только для веб-запросов)
        if not is_local_file:
            response.encoding = response.apparent_encoding
        
        # Парсинг HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Получение текста страницы
        page_text = soup.get_text(separator=' ', strip=True)

        # Переменные для хранения контекста
        context = None
        
        # Сначала проверяем точное совпадение
        search_text = text_to_find.strip()
        position = page_text.find(search_text)
        text_found = position != -1
        match_type = 'exact' if text_found else None
        
        if text_found:
            # Извлекаем контекст для точного совпадения
            before, found, after = extract_context(page_text, position, len(search_text))
            context = {
                'before': before,
                'found': found,
                'after': after
            }

        # Если точное совпадение не найдено, пробуем гибкий поиск
        if not text_found:
            # Уровень 1: Очистка БЕЗ удаления цифр
            cleaned_search_text = clean_text_for_search(text_to_find, remove_digits=False)
            cleaned_page_text = clean_text_for_search(page_text, remove_digits=False)
            
            # Логируем очищенный текст для отладки
            logger.debug(f"Original search text: '{text_to_find}'")
            logger.debug(f"Cleaned search text (level 1): '{cleaned_search_text}'")

            # Ищем очищенный текст в очищенной странице
            if cleaned_search_text and len(cleaned_search_text) > MIN_FUZZY_TEXT_LENGTH:
                # Сначала пробуем найти полное совпадение очищенного текста
                cleaned_position = cleaned_page_text.find(cleaned_search_text)
                text_found = cleaned_position != -1
                
                if text_found:
                    # Для fuzzy match нужно найти соответствующую позицию в оригинальном тексте
                    # Стратегия: найти фрагмент оригинального текста, который после очистки даст нашу находку
                    
                    # Берем несколько первых слов из найденной последовательности для более точного поиска
                    search_words = cleaned_search_text.split()[:3]  # Первые 3 слова
                    search_pattern = ' '.join(search_words)
                    
                    # Ищем этот паттерн во всех возможных местах оригинального текста
                    best_match_pos = -1
                    best_match_score = 0
                    
                    # Проходим по оригинальному тексту и ищем место, где после очистки будет наш паттерн
                    for i in range(len(page_text)):
                        # Берем фрагмент оригинального текста
                        fragment = page_text[i:i+len(cleaned_search_text)*3]
                        if not fragment:
                            continue
                        
                        # Очищаем фрагмент
                        cleaned_fragment = clean_text_for_search(fragment, remove_digits=False)
                        
                        # Проверяем, начинается ли очищенный фрагмент с нашего паттерна
                        if cleaned_fragment.startswith(search_pattern):
                            # Проверяем, сколько слов совпадает
                            fragment_words = cleaned_fragment.split()
                            match_count = 0
                            for j, word in enumerate(search_words):
                                if j < len(fragment_words) and fragment_words[j] == word:
                                    match_count += 1
                                else:
                                    break
                            
                            if match_count > best_match_score:
                                best_match_score = match_count
                                best_match_pos = i
                                
                                # Если нашли полное совпадение первых слов, можно остановиться
                                if match_count == len(search_words):
                                    break
                    
                    if best_match_pos != -1:
                        # Извлекаем контекст из оригинального текста
                        match_length = len(cleaned_search_text)
                        before, found, after = extract_context(page_text, best_match_pos, match_length)
                        context = {
                            'before': before,
                            'found': found,
                            'after': after,
                            'cleaned_search': cleaned_search_text,
                            'match_type': 'full_cleaned'
                        }
                        
                        logger.debug(f"Found position in original text: {best_match_pos}, pattern: '{search_pattern}'")

                # Если не найдено полное совпадение, ищем слова в правильном порядке
                if not text_found and len(cleaned_search_text.split()) >= 2:
                    # Разбиваем поисковый текст и текст страницы на слова
                    search_words_list = cleaned_search_text.split()
                    page_words_list = cleaned_page_text.split()
                    
                    # Ищем максимальную последовательность слов в правильном порядке
                    # Используем алгоритм поиска подпоследовательности с ограничением на пропуски
                    found_sequence = []
                    search_idx = 0
                    page_idx = 0
                    sequence_start_pos = -1
                    sequence_end_pos = -1
                    words_skipped = 0
                    
                    while search_idx < len(search_words_list) and page_idx < len(page_words_list):
                        if search_words_list[search_idx] == page_words_list[page_idx]:
                            # Нашли слово в правильном порядке
                            if sequence_start_pos == -1:
                                sequence_start_pos = page_idx
                            found_sequence.append(search_words_list[search_idx])
                            sequence_end_pos = page_idx
                            search_idx += 1
                            page_idx += 1
                            words_skipped = 0  # Сбрасываем счетчик пропусков
                        else:
                            # Слово не совпало, пропускаем слово на странице
                            page_idx += 1
                            words_skipped += 1
                            
                            # Если пропустили слишком много слов, начинаем заново
                            if words_skipped > MAX_WORDS_BETWEEN:
                                search_idx = 0
                                found_sequence = []
                                sequence_start_pos = -1
                                words_skipped = 0
                    
                    # Вычисляем процент найденных слов в порядке
                    match_ratio = len(found_sequence) / len(search_words_list) if len(search_words_list) > 0 else 0
                    
                    # Проверяем условия: процент И абсолютный минимум слов в последовательности
                    if (match_ratio >= MIN_MATCH_RATIO and 
                        len(found_sequence) >= MIN_WORDS_IN_SEQUENCE):
                        text_found = True
                        
                        missing_words = [w for w in search_words_list if w not in found_sequence]
                        
                        logger.debug(f"Fuzzy match found via sequence: {found_sequence} from '{cleaned_search_text}' "
                                   f"(match ratio: {match_ratio:.2%}, {len(found_sequence)}/{len(search_words_list)} words in order)")
                        
                        if sequence_start_pos != -1 and found_sequence:
                            # Находим позицию в оригинальном тексте по первому слову последовательности
                            first_word = found_sequence[0]
                            
                            # Ищем первое слово в оригинальном тексте (игнорируя регистр)
                            original_position = page_text.lower().find(first_word.lower())
                            
                            if original_position != -1:
                                # Вычисляем длину найденной последовательности
                                match_length = len(' '.join(found_sequence)) * 2
                                
                                before, found, after = extract_context(page_text, original_position, match_length)
                                
                                context = {
                                    'before': before,
                                    'found': found,
                                    'after': after,
                                    'found_words': found_sequence,
                                    'missing_words': missing_words if missing_words else None,
                                    'match_ratio': f"{match_ratio:.0%}"
                                }
                                
                                logger.debug(f"Found sequence position in original text: {original_position}, first word: '{first_word}'")

                if text_found:
                    match_type = 'fuzzy'
                    logger.debug(f"Fuzzy match found: '{cleaned_search_text}' in cleaned page text")
                
                # Уровень 2: Если не найдено, пробуем с удалением цифр
                if not text_found:
                    logger.debug("Level 1 search failed, trying level 2 (remove digits)")
                    
                    cleaned_search_text_no_digits = clean_text_for_search(text_to_find, remove_digits=True)
                    cleaned_page_text_no_digits = clean_text_for_search(page_text, remove_digits=True)
                    
                    logger.debug(f"Cleaned search text (level 2, no digits): '{cleaned_search_text_no_digits}'")
                    
                    if cleaned_search_text_no_digits and len(cleaned_search_text_no_digits) > MIN_FUZZY_TEXT_LENGTH:
                        # Проверяем полное совпадение без цифр
                        cleaned_position = cleaned_page_text_no_digits.find(cleaned_search_text_no_digits)
                        text_found = cleaned_position != -1
                        
                        if text_found:
                            # Находим позицию по первому слову
                            first_word = cleaned_search_text_no_digits.split()[0] if cleaned_search_text_no_digits.split() else ""
                            
                            if first_word:
                                # Ищем первое слово в оригинальном тексте
                                approx_start = max(0, cleaned_position - 100)
                                search_area = page_text[approx_start:]
                                original_position = search_area.lower().find(first_word.lower())
                                
                                if original_position != -1:
                                    original_position += approx_start
                                    
                                    match_length = len(cleaned_search_text_no_digits)
                                    before, found, after = extract_context(page_text, original_position, match_length)
                                    context = {
                                        'before': before,
                                        'found': found,
                                        'after': after,
                                        'cleaned_search': cleaned_search_text_no_digits,
                                        'match_type': 'full_cleaned_no_digits'
                                    }
                                    match_type = 'fuzzy'
                                    logger.debug(f"Fuzzy match found (level 2): '{cleaned_search_text_no_digits}', first word: '{first_word}'")
                        
                        # Если полное совпадение не найдено, ищем последовательность без цифр
                        if not text_found and len(cleaned_search_text_no_digits.split()) >= 2:
                            search_words_list = cleaned_search_text_no_digits.split()
                            page_words_list = cleaned_page_text_no_digits.split()
                            
                            found_sequence = []
                            search_idx = 0
                            page_idx = 0
                            sequence_start_pos = -1
                            sequence_end_pos = -1
                            words_skipped = 0
                            
                            while search_idx < len(search_words_list) and page_idx < len(page_words_list):
                                if search_words_list[search_idx] == page_words_list[page_idx]:
                                    if sequence_start_pos == -1:
                                        sequence_start_pos = page_idx
                                    found_sequence.append(search_words_list[search_idx])
                                    sequence_end_pos = page_idx
                                    search_idx += 1
                                    page_idx += 1
                                    words_skipped = 0
                                else:
                                    page_idx += 1
                                    words_skipped += 1
                                    
                                    if words_skipped > MAX_WORDS_BETWEEN:
                                        search_idx = 0
                                        found_sequence = []
                                        sequence_start_pos = -1
                                        words_skipped = 0
                            
                            match_ratio = len(found_sequence) / len(search_words_list) if len(search_words_list) > 0 else 0
                            
                            if (match_ratio >= MIN_MATCH_RATIO and 
                                len(found_sequence) >= MIN_WORDS_IN_SEQUENCE):
                                text_found = True
                                missing_words = [w for w in search_words_list if w not in found_sequence]
                                
                                logger.debug(f"Fuzzy match found via sequence (level 2): {found_sequence} "
                                           f"(match ratio: {match_ratio:.2%}, {len(found_sequence)}/{len(search_words_list)} words in order)")
                                
                                if sequence_start_pos != -1 and found_sequence:
                                    # Находим позицию по первому слову последовательности
                                    first_word = found_sequence[0]
                                    
                                    # Ищем первое слово в оригинальном тексте
                                    original_position = page_text.lower().find(first_word.lower())
                                    
                                    if original_position != -1:
                                        match_length = len(' '.join(found_sequence)) * 2
                                        
                                        before, found, after = extract_context(page_text, original_position, match_length)
                                        
                                        context = {
                                            'before': before,
                                            'found': found,
                                            'after': after,
                                            'found_words': found_sequence,
                                            'missing_words': missing_words if missing_words else None,
                                            'match_ratio': f"{match_ratio:.0%}",
                                            'cleaned_search': cleaned_search_text_no_digits
                                        }
                                        match_type = 'fuzzy'
                                        
                                        logger.debug(f"Found sequence (level 2) position in original text: {original_position}, first word: '{first_word}'")

        return text_found, None, match_type, context
        
    except requests.exceptions.Timeout:
        return False, f"Timeout when accessing {url}", None, None
    except requests.exceptions.ConnectionError:
        return False, f"Connection error to {url}", None, None
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP error {e.response.status_code} for {url}", None, None
    except requests.exceptions.RequestException as e:
        return False, f"Request error: {str(e)}", None, None
    except Exception as e:
        return False, f"Unexpected error: {str(e)}", None, None

def process_excel_file(filename, delay=1, sheet_names=None, csv_output=None):
    """
    Обрабатывает Excel файл и проверяет ссылки

    Args:
        filename: Путь к Excel файлу
        delay: Задержка между запросами в секундах
        sheet_names: Имена или индексы листов для обработки (None - все листы)
        csv_output: Путь к CSV файлу для сохранения найденных совпадений (None - не сохранять)
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
        found_matches = []  # Список найденных совпадений для CSV

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
                
                # Показываем очищенный текст для fuzzy поиска
                cleaned = clean_text_for_search(text)
                if cleaned != text:
                    logger.info(f"Cleaned text for fuzzy search: {cleaned[:100]}{'...' if len(cleaned) > 100 else ''}")
                
                logger.info(f"Link: {link}")

                # Проверка наличия текста на странице
                found, error, match_type, context = check_text_on_page(link, text)

                if error:
                    logger.error(f"Row {idx} (sheet '{sheet_name}'): ERROR - {error}")
                    error_count += 1
                elif found:
                    match_indicator = "✓" if match_type == 'exact' else "≈"
                    match_desc = "exact match" if match_type == 'exact' else "fuzzy match"
                    logger.info(f"Row {idx} (sheet '{sheet_name}'): {match_indicator} FOUND ({match_desc}) - Text present on page")
                    
                    # Логируем контекст найденного текста
                    if context:
                        logger.info(f"  Context:")
                        if context.get('before'):
                            logger.info(f"    Before: ...{context['before'][-200:]}")
                        logger.info(f"    Found:  [{context.get('found', '')}]")
                        if context.get('after'):
                            logger.info(f"    After:  {context['after'][:200]}...")
                        if 'cleaned_search' in context:
                            logger.info(f"    Cleaned search text: {context['cleaned_search']}")
                        if 'found_words' in context:
                            logger.info(f"    Found words (in order): {', '.join(context['found_words'])}")
                        if 'missing_words' in context and context['missing_words']:
                            logger.info(f"    Missing words: {', '.join(context['missing_words'])}")
                        if 'match_ratio' in context:
                            logger.info(f"    Match ratio: {context['match_ratio']}")

                    # Добавляем найденное совпадение в список для CSV
                    found_matches.append((link, text))

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

        # Сохраняем найденные совпадения в CSV, если указан файл вывода
        if csv_output and found_matches:
            if save_found_matches_to_csv(found_matches, csv_output):
                logger.info(f"Found {len(found_matches)} matches saved to CSV: {csv_output}")
            else:
                logger.error("Failed to save matches to CSV file")

        # Итоговая статистика по всем листам
        logger.info(f"\n{'='*80}")
        logger.info("FINAL STATISTICS FOR ALL SHEETS:")
        logger.info(f"Total rows processed: {total_rows_all}")
        logger.info(f"Text found: {found_count_all}")
        logger.info(f"Text not found: {not_found_count_all}")
        logger.info(f"Processing errors: {error_count_all}")
        logger.info(f"Results saved to file: {log_filename}")
        if csv_output and found_matches:
            logger.info(f"Found matches saved to CSV: {csv_output}")

    except FileNotFoundError:
        logger.error(f"File {filename} not found")
    except Exception as e:
        logger.error(f"Critical error processing file: {str(e)}")
        raise

def check_single_url(url, text, csv_output=None):
    """
    Проверяет наличие текста на одной веб-странице

    Args:
        url: URL для проверки
        text: Текст для поиска
        csv_output: Путь к CSV файлу для сохранения результата
    """
    logger.info("="*80)
    logger.info("Single URL check mode")
    logger.info(f"URL: {url}")
    logger.info(f"Text to search: {text[:100]}{'...' if len(text) > 100 else ''}")
    
    # Показываем очищенный текст для fuzzy поиска
    cleaned = clean_text_for_search(text)
    if cleaned != text.strip():
        logger.info(f"Cleaned text for fuzzy search: {cleaned[:100]}{'...' if len(cleaned) > 100 else ''}")
    
    logger.info("="*80)
    
    # Проверка наличия текста на странице
    found, error, match_type, context = check_text_on_page(url, text)
    
    if error:
        logger.error(f"ERROR: {error}")
        print(f"\n❌ ERROR: {error}")
        return False
    elif found:
        match_indicator = "✓" if match_type == 'exact' else "≈"
        match_desc = "exact match" if match_type == 'exact' else "fuzzy match"
        logger.info(f"{match_indicator} FOUND ({match_desc}) - Text present on page")
        print(f"\n✓ FOUND ({match_desc}) - Text present on page")
        
        # Логируем контекст найденного текста
        if context:
            logger.info(f"\nContext:")
            if context.get('before'):
                logger.info(f"  Before: ...{context['before'][-200:]}")
            logger.info(f"  Found:  [{context.get('found', '')}]")
            if context.get('after'):
                logger.info(f"  After:  {context['after'][:200]}...")
            if 'common_words' in context:
                logger.info(f"  Common words: {', '.join(context['common_words'])}")
            
            # Выводим контекст в консоль
            print(f"\nContext:")
            if context.get('before'):
                print(f"  Before: ...{context['before'][-200:]}")
            print(f"  Found:  [{context.get('found', '')}]")
            if context.get('after'):
                print(f"  After:  {context['after'][:200]}...")
            if 'cleaned_search' in context:
                print(f"  Cleaned search text: {context['cleaned_search']}")
            if 'found_words' in context:
                print(f"  Found words (in order): {', '.join(context['found_words'])}")
            if 'missing_words' in context and context['missing_words']:
                print(f"  Missing words: {', '.join(context['missing_words'])}")
            if 'match_ratio' in context:
                print(f"  Match ratio: {context['match_ratio']}")
        
        # Сохраняем в CSV если указано
        if csv_output:
            if save_found_matches_to_csv([(url, text)], csv_output):
                logger.info(f"Result saved to CSV: {csv_output}")
                print(f"Result saved to CSV: {csv_output}")
        
        return True
    else:
        logger.warning("✗ NOT FOUND - Text absent from page")
        print("\n✗ NOT FOUND - Text absent from page")
        return False

def main():
    """Главная функция"""
    # Настройка парсера аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Check text from column G on web pages using links from column I',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Excel file mode
  python check_links.py data.xlsx  # Default logs in logs/ directory
  python check_links.py data.xlsx --log-dir my_logs  # Custom log directory
  python check_links.py data.xlsx --all-sheets  # Process all sheets
  
  # Direct URL check mode
  python check_links.py --url https://example.com --text "Search text"  # Check single URL
  python check_links.py --url https://example.com --text "Text" --log-dir ./logs  # With custom log dir
        """
    )
    
    # Создаем группу взаимоисключающих аргументов для режима работы
    mode_group = parser.add_mutually_exclusive_group(required=True)
    
    mode_group.add_argument(
        'excel_file',
        nargs='?',
        type=str,
        help='Path to Excel file to process'
    )
    
    mode_group.add_argument(
        '--url',
        type=str,
        help='Single URL to check (requires --text)'
    )
    
    parser.add_argument(
        '--text',
        type=str,
        help='Text to search on the page (used with --url)'
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

    parser.add_argument(
        '-o', '--output-csv',
        type=str,
        nargs='?',
        const=None,
        help='Output CSV file to save found matches (auto-generated if not specified)'
    )

    parser.add_argument(
        '-l', '--log-dir',
        type=str,
        help='Directory for log files (default: logs/)'
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
    
    # Проверка аргументов в зависимости от режима работы
    if args.url:
        # Режим проверки одного URL
        if not args.text:
            parser.error("--url requires --text argument")
            sys.exit(1)
    elif args.excel_file:
        # Режим обработки Excel файла
        if args.text:
            parser.error("--text can only be used with --url")
            sys.exit(1)
        
        # Проверка существования файла
        if not os.path.exists(args.excel_file):
            print(f"Error: File '{args.excel_file}' not found!")
            sys.exit(1)
    else:
        parser.error("Either excel_file or --url with --text must be provided")
        sys.exit(1)

    # Настраиваем логирование
    setup_logging(args.log_dir)

    # Обработка режима с одиночным URL
    if args.url:
        # Автоматическая генерация имени CSV файла для одиночного URL
        csv_output = args.output_csv
        if csv_output is None and args.output_csv != '':
            # Не генерируем автоматически для одиночного URL, только если явно указано
            csv_output = None
        
        print(f"Log file: {log_filename}")
        if csv_output:
            print(f"CSV output: {csv_output}")
        print("")
        
        # Проверяем одиночный URL
        success = check_single_url(args.url, args.text, csv_output)
        
        print(f"\nLog saved to: {log_filename}")
        sys.exit(0 if success else 1)
    
    # Режим обработки Excel файла
    # Автоматическая генерация имени CSV файла, если не указано
    csv_output = args.output_csv
    if csv_output is None:
        # Получаем имя файла без расширения и добавляем суффикс
        base_name = os.path.splitext(args.excel_file)[0]
        csv_output = f"{base_name}_found_matches.csv"

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
    print(f"Log directory: {os.path.dirname(log_filename)}")
    print(f"Log file: {os.path.basename(log_filename)}")
    print(f"CSV output: {csv_output}")
    print(f"Delay between requests: {args.delay} sec")
    print(f"Request timeout: {args.timeout} sec")
    print("="*80)
    print()

    # Запуск обработки
    process_excel_file(args.excel_file, delay=args.delay, sheet_names=sheet_names, csv_output=csv_output)

    print()
    print("="*80)
    print("Processing completed!")
    print(f"Detailed results in file: {log_filename}")
    print("="*80)

if __name__ == "__main__":
    main()


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
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse, urlsplit
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

# Минимальная длина нормализованного message-поля для срабатывания префиксного
# (truncated) совпадения — защита от ложных ЖИВ по слишком короткому фрагменту.
MIN_MATCH_CHARS = 12

# Статусы проверки (единые во всём пайплайне)
STATUS_ALIVE = "ЖИВ"
STATUS_DELETED = "УДАЛЁН"
STATUS_ERROR = "ОШИБКА"

# Хосты Telegram, для которых текст сообщения берётся из message-полей (og/виджет),
# а не из soup.get_text() (там — заглушка «View in Telegram»).
TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog"}

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

# ============================================================================
# ЧИСТЫЕ ФУНКЦИИ СОПОСТАВЛЕНИЯ (без сети — тестируются в test_matching.py)
# ============================================================================

# Всё, кроме букв (лат./кир.) и цифр, считаем разделителем.
_NORMALIZE_RE = re.compile(r'[^0-9a-zA-Zа-яёА-ЯЁ]+')


def normalize(text):
    """
    Нормализует текст для сравнения: нижний регистр, все спецсимволы/пунктуация →
    пробел (кириллица сохраняется), повторные пробелы схлопываются.
    """
    if not text:
        return ""
    return _NORMALIZE_RE.sub(' ', str(text).lower()).strip()


def is_telegram_host(url):
    """True, если URL ведёт на страницу сообщения Telegram (t.me / telegram.me)."""
    try:
        host = urlsplit(str(url)).netloc.lower().split(':')[0]
    except Exception:
        return False
    if host.startswith('www.'):
        host = host[4:]
    return host in TELEGRAM_HOSTS


def extract_page_message(soup):
    """
    Извлекает высокосигнальные «поля сообщения» страницы.

    Для Telegram текст поста/комментария лежит в og:description (а НЕ в видимом
    теле страницы). og:title намеренно НЕ берём — это имя канала/автора.

    Returns:
        dict: {'message_fields': [str, ...], 'body_text': str}
    """
    message_fields = []

    og_desc = soup.find('meta', attrs={'property': 'og:description'})
    if og_desc and og_desc.get('content'):
        message_fields.append(og_desc['content'])

    tw_desc = soup.find('meta', attrs={'name': 'twitter:description'})
    if tw_desc and tw_desc.get('content'):
        message_fields.append(tw_desc['content'])

    for div in soup.select('div.tgme_widget_message_text'):
        txt = div.get_text(' ', strip=True)
        if txt:
            message_fields.append(txt)

    body_text = soup.get_text(separator=' ', strip=True)
    return {'message_fields': message_fields, 'body_text': body_text}


def _fuzzy_sequence_match(search_words, page_words):
    """
    Ищет максимальную последовательность слов search_words в page_words в том же
    порядке, с допуском не более MAX_WORDS_BETWEEN лишних слов между совпадениями.

    Returns:
        list: найденная последовательность слов (по порядку).
    """
    found_sequence = []
    search_idx = 0
    page_idx = 0
    words_skipped = 0

    while search_idx < len(search_words) and page_idx < len(page_words):
        if search_words[search_idx] == page_words[page_idx]:
            found_sequence.append(search_words[search_idx])
            search_idx += 1
            page_idx += 1
            words_skipped = 0
        else:
            page_idx += 1
            words_skipped += 1
            if words_skipped > MAX_WORDS_BETWEEN:
                search_idx = 0
                found_sequence = []
                words_skipped = 0

    return found_sequence


def _match_body_legacy(search_text, body_text):
    """
    Legacy-сопоставление по видимому тексту страницы (не-Telegram / локальные файлы).
    Сохраняет прежнее поведение: точное вхождение, затем нечёткий поиск
    последовательности слов (с очисткой, два уровня — без/с удалением цифр).

    Returns:
        tuple: (found: bool, match_type: str|None, snippet: str|None)
    """
    if search_text and search_text.strip() and search_text.strip() in body_text:
        pos = body_text.find(search_text.strip())
        _, found, _ = extract_context(body_text, pos, len(search_text.strip()))
        return True, 'exact', found

    for remove_digits in (False, True):
        cleaned_search = clean_text_for_search(search_text, remove_digits=remove_digits)
        cleaned_page = clean_text_for_search(body_text, remove_digits=remove_digits)
        if not cleaned_search or len(cleaned_search) <= MIN_FUZZY_TEXT_LENGTH:
            continue

        if cleaned_search in cleaned_page:
            return True, 'fuzzy', cleaned_search

        search_words = cleaned_search.split()
        if len(search_words) < 2:
            continue
        found_sequence = _fuzzy_sequence_match(search_words, cleaned_page.split())
        ratio = len(found_sequence) / len(search_words) if search_words else 0
        if ratio >= MIN_MATCH_RATIO and len(found_sequence) >= MIN_WORDS_IN_SEQUENCE:
            return True, 'fuzzy', ' '.join(found_sequence)

    return False, None, None


def match_message(search_text, message_fields, body_text, is_telegram):
    """
    Чистое сопоставление искомого текста со страницей. Без сети.

    Telegram: ищем ТОЛЬКО в message-полях (og:description/twitter/виджет) —
      - ng in nc                          → ЖИВ (meta_exact)
      - len(nc)>=MIN_MATCH_CHARS и ng.startswith(nc) → ЖИВ (meta_truncated, og обрезан)
      Никакого нечёткого поиска по message-полям (избегаем ложных ЖИВ).
      Нет ни одного непустого message-поля → ОШИБКА (заглушка/login/антибот).
      Есть поле(я), но текст не совпал → УДАЛЁН.
    Не-Telegram / локальный файл: legacy-поиск по body_text (exact + fuzzy);
      найдено → ЖИВ, иначе УДАЛЁН.

    Returns:
        tuple: (status, match_type, snippet)
    """
    ng = normalize(search_text)
    if not ng:
        return STATUS_ERROR, None, None

    if is_telegram:
        nonempty = [(f, normalize(f)) for f in message_fields if normalize(f)]
        if not nonempty:
            return STATUS_ERROR, None, None
        for original, nc in nonempty:
            if ng in nc:
                return STATUS_ALIVE, 'meta_exact', original.strip()
            if len(nc) >= MIN_MATCH_CHARS and ng.startswith(nc):
                return STATUS_ALIVE, 'meta_truncated', original.strip()
        return STATUS_DELETED, None, None

    found, match_type, snippet = _match_body_legacy(search_text, body_text)
    return (STATUS_ALIVE if found else STATUS_DELETED), match_type, snippet


# ============================================================================
# TELEGRAM: проверка по реально отображаемому контенту (embed-виджет)
# ============================================================================
# og:description — это закэшированное превью, которое СОХРАНЯЕТСЯ после удаления
# поста, поэтому ему доверять нельзя (давало ложные ЖИВ). Истинный признак
# «сообщение отображается сейчас» — текст в div.tgme_widget_message_text на
# странице ?embed=1.

def build_embed_url(url):
    """Добавляет embed=1 к URL (сохраняя существующие query-параметры)."""
    return url + ('&' if '?' in url else '?') + 'embed=1'


def extract_telegram_widget_texts(soup):
    """
    Из embed-страницы Telegram достаёт реально отрендеренный контент.

    Returns:
        tuple: (widget_texts: [str], n_wrappers: int, is_private: bool)
        n_wrappers — число div.tgme_widget_message (0 → ничего не отрендерено:
        приватный канал/login/интерстишл).
    """
    widget_texts = [t.get_text(' ', strip=True)
                    for t in soup.select('div.tgme_widget_message_text')]
    widget_texts = [t for t in widget_texts if t]
    n_wrappers = len(soup.select('div.tgme_widget_message'))
    low = soup.get_text(' ', strip=True).lower()
    is_private = ('private group or channel' in low) or ('only work if you are a member' in low)
    return widget_texts, n_wrappers, is_private


def match_telegram_widgets(search_text, widget_texts):
    """
    Совпадает ли искомый текст с одним из реально отображаемых виджетов.
    Виджет содержит ПОЛНЫЙ текст сообщения, поэтому ищем вхождение G в виджет
    (ng in nt) либо строгий префикс (на случай, если виджет короче).

    Returns:
        tuple: (found: bool, snippet: str|None)
    """
    ng = normalize(search_text)
    if not ng:
        return False, None
    for t in widget_texts:
        nt = normalize(t)
        if not nt:
            continue
        if ng in nt or (len(nt) >= MIN_MATCH_CHARS and ng.startswith(nt)):
            return True, t.strip()
    return False, None


def decide_telegram_status(search_text, widget_texts, n_wrappers, is_private):
    """
    Чистое решение статуса для Telegram по данным одной embed-страницы.

    - ЖИВ      — G найден в одном из виджетов (реально отображается);
    - ОШИБКА   — приватный канал/группа ИЛИ нет ни одной обёртки сообщения
                 (login/интерстишл/нестандартная вёрстка) — проверить нельзя;
    - УДАЛЁН   — обёртка есть, но текст G не отображается (удалён/скрыт/изменён).

    Returns:
        tuple: (status, match_type, snippet)
    """
    found, snippet = match_telegram_widgets(search_text, widget_texts)
    if found:
        return STATUS_ALIVE, 'tg_widget', snippet
    if is_private or n_wrappers == 0:
        return STATUS_ERROR, None, None
    return STATUS_DELETED, None, None


def escape_csv_cell(value):
    """
    Защита от CSV-инъекций в Excel: значения, начинающиеся с = + - @ TAB CR,
    префиксуются апострофом (OWASP CSV injection).
    """
    s = "" if value is None else str(value)
    if s and s[0] in ('=', '+', '-', '@', '\t', '\r'):
        return "'" + s
    return s


class RateLimiter:
    """
    Глобальный ограничитель частоты запросов с адаптивным замедлением.

    - Разносит СТАРТЫ запросов минимум на 1/rate секунд (+ небольшой джиттер).
    - При сериях троттлинга (429/timeout/conn) удваивает интервал (cap 5с).
    - После окна успехов постепенно возвращает базовый темп.
    """

    def __init__(self, rate, jitter=(0.0, 0.0)):
        self._base_interval = (1.0 / rate) if rate and rate > 0 else 0.0
        self._min_interval = self._base_interval
        self._jitter = jitter
        self._next = 0.0
        self._lock = threading.Lock()
        self._consec_throttle = 0
        self._consec_ok = 0

    def acquire(self):
        if self._base_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            extra = random.uniform(*self._jitter) if self._jitter[1] > 0 else 0.0
            self._next = start + self._min_interval + extra
        wait = start - time.monotonic()
        if wait > 0:
            time.sleep(wait)

    def report(self, throttled):
        with self._lock:
            if throttled:
                self._consec_throttle += 1
                self._consec_ok = 0
                if self._consec_throttle >= 5:
                    self._min_interval = min(max(self._min_interval, 0.05) * 2, 5.0)
                    self._consec_throttle = 0
            else:
                self._consec_ok += 1
                self._consec_throttle = 0
                if self._consec_ok >= 20 and self._min_interval > self._base_interval:
                    self._min_interval = max(self._base_interval, self._min_interval / 2)
                    self._consec_ok = 0


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

def _sleep_backoff(attempt):
    """Экспоненциальный бэкофф 1с/3с/6с (+джиттер), cap 60с."""
    base = [1, 3, 6]
    delay = min(60, base[min(attempt, len(base) - 1)]) + random.uniform(0, 0.5)
    time.sleep(delay)


# Маркеры транзиентных (повторяемых) ошибок — в отличие от детерминантных
# (приватный канал / нет текста сообщения), которые повторять бессмысленно.
_TRANSIENT_MARKERS = ('Timeout', 'ConnectionError', 'HTTP 4', 'HTTP 5',
                      'Request error', 'Unknown error', 'HTTP error')


def _is_transient_error(error_msg):
    """True, если ошибку имеет смысл повторить (сетевая/троттлинг), а не детерминантную."""
    if not error_msg:
        return False
    return any(marker in error_msg for marker in _TRANSIENT_MARKERS)


def _fetch_embed(embed_url, timeout, getter, retries, rate_limiter):
    """
    Скачивает embed-страницу Telegram с сетевыми ретраями/троттлингом.

    Returns:
        tuple: (status_code|None, widget_texts, n_wrappers, is_private, error)
        error != None → сетевой/транзиентный сбой (повторять имеет смысл).
    """
    last_error = None
    for attempt in range(retries + 1):
        if rate_limiter is not None:
            rate_limiter.acquire()
        try:
            response = getter.get(embed_url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            sc = response.status_code
            if sc == 429 or sc >= 500:
                if rate_limiter is not None:
                    rate_limiter.report(True)
                last_error = f"HTTP {sc} for {embed_url}"
                if attempt < retries:
                    _sleep_backoff(attempt)
                    continue
                return None, [], 0, False, last_error
            response.raise_for_status()
            if rate_limiter is not None:
                rate_limiter.report(False)
            if response.encoding is None:
                response.encoding = response.apparent_encoding or 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            texts, n_wrappers, is_private = extract_telegram_widget_texts(soup)
            return sc, texts, n_wrappers, is_private, None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if rate_limiter is not None:
                rate_limiter.report(True)
            last_error = f"{type(e).__name__} for {embed_url}"
            if attempt < retries:
                _sleep_backoff(attempt)
                continue
            return None, [], 0, False, last_error
        except requests.exceptions.HTTPError as e:
            sc = e.response.status_code if e.response is not None else '?'
            return None, [], 0, False, f"HTTP error {sc} for {embed_url}"
        except requests.exceptions.RequestException as e:
            last_error = f"Request error: {e}"
            if attempt < retries:
                _sleep_backoff(attempt)
                continue
            return None, [], 0, False, last_error
        except Exception as e:
            return None, [], 0, False, f"Unexpected error: {e}"
    return None, [], 0, False, last_error or "Unknown error"


def _check_telegram(url, text_to_find, timeout, getter, retries, rate_limiter):
    """
    Проверка Telegram-ссылки по embed-виджету (реально отображаемый контент).
    Для comment/thread-ссылок окно соседних комментов у Telegram слегка
    недетерминировано → на «не совпало» делаем 1 повторный embed-запрос,
    чтобы не потерять живой комментарий.
    """
    embed_url = build_embed_url(url)
    is_comment = ('comment=' in url) or ('thread=' in url)
    is_private = urlsplit(url).path.startswith('/c/')
    attempts = 2 if is_comment else 1

    widget_texts, n_wrappers = [], 0
    for _ in range(attempts):
        sc, widget_texts, n_wrappers, priv, error = _fetch_embed(
            embed_url, timeout, getter, retries, rate_limiter)
        if error:
            return STATUS_ERROR, error, None, None
        is_private = is_private or priv
        status, match_type, snippet = decide_telegram_status(
            text_to_find, widget_texts, n_wrappers, is_private)
        if status == STATUS_ALIVE:
            return STATUS_ALIVE, None, match_type, snippet

    # После всех попыток совпадения нет
    status, match_type, snippet = decide_telegram_status(
        text_to_find, widget_texts, n_wrappers, is_private)
    if status == STATUS_ERROR:
        reason = ("Приватный канал/группа — проверить вручную (нужно членство)"
                  if is_private else
                  "Нет отображаемого сообщения (login/интерстишл/изменённая вёрстка)")
        return STATUS_ERROR, reason, None, None
    return status, None, match_type, snippet  # STATUS_DELETED


def check_text_on_page(url, text_to_find, timeout=10, session=None, retries=0, rate_limiter=None):
    """
    Проверяет наличие текста на веб-странице (или в локальном файле).

    Для Telegram текст ищется в message-полях (og:description/виджет), т.к. в
    soup.get_text() лежит лишь заглушка «View in Telegram». Для остальных хостов —
    legacy-поиск по видимому тексту.

    Args:
        url: URL страницы или путь к локальному файлу
        text_to_find: Текст для поиска
        timeout: Таймаут запроса в секундах
        session: requests.Session (None → модульный requests)
        retries: число дополнительных попыток при троттлинге/сбое сети
        rate_limiter: общий RateLimiter (None → без ограничения)

    Returns:
        tuple: (status, error, match_type, snippet)
        status: STATUS_ALIVE / STATUS_DELETED / STATUS_ERROR
    """
    parsed = urlparse(url)
    telegram = is_telegram_host(url)

    # ---- Локальный файл (поведение сохранено) ----
    if parsed.scheme not in ('http', 'https') and (parsed.scheme == 'file' or os.path.exists(url)):
        try:
            if parsed.scheme == 'file':
                file_path = parsed.path
                if file_path.startswith('/') and os.name == 'nt':
                    file_path = file_path[1:]
            else:
                file_path = url
            if not os.path.exists(file_path):
                return STATUS_ERROR, f"Local file not found: {file_path}", None, None
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            soup = BeautifulSoup(content, 'html.parser')
            fields = extract_page_message(soup)
            status, match_type, snippet = match_message(
                text_to_find, fields['message_fields'], fields['body_text'], telegram)
            return status, None, match_type, snippet
        except Exception as e:
            return STATUS_ERROR, f"Local file error: {e}", None, None

    if parsed.scheme not in ('http', 'https'):
        return STATUS_ERROR, "Invalid URL or file path", None, None

    # ---- Веб-запрос с ретраями и адаптивным троттлингом ----
    getter = session if session is not None else requests

    # Telegram: проверяем по реально отображаемому контенту (embed-виджет),
    # а не по кэшируемому og:description.
    if telegram:
        return _check_telegram(url, text_to_find, timeout, getter, retries, rate_limiter)

    last_error = None
    for attempt in range(retries + 1):
        if rate_limiter is not None:
            rate_limiter.acquire()
        try:
            response = getter.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            sc = response.status_code
            if sc == 429 or sc >= 500:
                if rate_limiter is not None:
                    rate_limiter.report(True)
                last_error = f"HTTP {sc} for {url}"
                if attempt < retries:
                    _sleep_backoff(attempt)
                    continue
                return STATUS_ERROR, last_error, None, None
            response.raise_for_status()
            if rate_limiter is not None:
                rate_limiter.report(False)
            if response.encoding is None:
                response.encoding = response.apparent_encoding or 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            fields = extract_page_message(soup)
            status, match_type, snippet = match_message(
                text_to_find, fields['message_fields'], fields['body_text'], telegram)
            err = None
            if status == STATUS_ERROR:
                body_low = fields['body_text'].lower()
                is_private = (
                    urlsplit(url).path.startswith('/c/')
                    or 'private group or channel' in body_low
                    or 'only work if you are a member' in body_low
                )
                err = ("Приватный канал/группа — проверить вручную (нужно членство)"
                       if is_private
                       else "Нет текста сообщения на странице (заглушка/login/изменённая вёрстка)")
            return status, err, match_type, snippet
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if rate_limiter is not None:
                rate_limiter.report(True)
            last_error = f"{type(e).__name__} for {url}"
            if attempt < retries:
                _sleep_backoff(attempt)
                continue
            return STATUS_ERROR, last_error, None, None
        except requests.exceptions.HTTPError as e:
            sc = e.response.status_code if e.response is not None else '?'
            # 4xx (кроме 429) — окончательный ответ сервера, не ретраим
            return STATUS_ERROR, f"HTTP error {sc} for {url}", None, None
        except requests.exceptions.RequestException as e:
            last_error = f"Request error: {e}"
            if attempt < retries:
                _sleep_backoff(attempt)
                continue
            return STATUS_ERROR, last_error, None, None
        except Exception as e:
            return STATUS_ERROR, f"Unexpected error: {e}", None, None

    return STATUS_ERROR, last_error or "Unknown error", None, None


_thread_local = threading.local()


def _get_session():
    """Возвращает requests.Session, локальную для текущего потока."""
    s = getattr(_thread_local, 'session', None)
    if s is None:
        s = requests.Session()
        _thread_local.session = s
    return s


def write_results_csv(records, csv_filename):
    """
    Пишет полный отчёт по всем строкам-сообщениям (UTF-8 с BOM, защита от
    CSV-инъекций). Заменяет прежний found-only CSV.
    """
    headers = ['Строка', 'Тип', 'Автор', 'URL', 'Текст',
               'Статус', 'ТипСовпадения', 'НайденныйФрагмент']
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL, escapechar='\\')
            writer.writerow(headers)
            for r in records:
                writer.writerow([
                    escape_csv_cell(r.get('row', '')),
                    escape_csv_cell(r.get('type', '')),
                    escape_csv_cell(r.get('author', '')),
                    escape_csv_cell(r.get('url', '')),
                    escape_csv_cell(r.get('text', '')),
                    escape_csv_cell(r.get('status', '')),
                    escape_csv_cell(r.get('match_type') or ''),
                    escape_csv_cell(r.get('snippet') or ''),
                ])
        logger.info(f"Results CSV saved: {csv_filename}")
        return True
    except Exception as e:
        logger.error(f"Error writing results CSV: {e}")
        return False


def _process_record(rec, timeout, retries, rate_limiter):
    """Проверяет одну строку-сообщение; результат пишется в rec (in place)."""
    status, error, match_type, snippet = check_text_on_page(
        rec['url'], rec['text'], timeout=timeout, session=_get_session(),
        retries=retries, rate_limiter=rate_limiter)
    rec['status'] = status
    rec['match_type'] = match_type
    rec['error'] = error
    if status == STATUS_ERROR:
        rec['snippet'] = error or ''
    else:
        rec['snippet'] = snippet or ''
    return rec


def _run_pass(recs, n_workers, timeout, retries, rate_limiter, label):
    """Прогоняет список записей через пул потоков; обновляет записи на месте."""
    total = len(recs)
    if total == 0:
        return
    done = 0
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = [ex.submit(_process_record, r, timeout, retries, rate_limiter)
                   for r in recs]
        for _ in as_completed(futures):
            done += 1
            if done % 50 == 0 or done == total:
                logger.info(f"[{label}] {done}/{total} processed")


def _build_records(sheets_dict):
    """
    Из листов Excel собирает список строк-сообщений (непустые G и I, без
    литеральной шапки 'Текст'/'Url'). Колонки Тип(K)/Автор(P) опциональны.
    Excel-строка = позиция в df + 2 (read_excel header=0 съедает первую строку листа).
    """
    records = []
    for sheet_name, df in sheets_dict.items():
        ncols = df.shape[1]
        if ncols < 9:
            logger.error(f"Sheet '{sheet_name}': fewer than 9 columns - skipped")
            continue
        text_col = df.iloc[:, 6]
        link_col = df.iloc[:, 8]
        type_col = df.iloc[:, 10] if ncols > 10 else None
        author_col = df.iloc[:, 15] if ncols > 15 else None

        for pos in range(len(df)):
            text = text_col.iloc[pos]
            link = link_col.iloc[pos]
            if pd.isna(text) or pd.isna(link):
                continue
            text = str(text).strip()
            link = str(link).strip()
            if not text or not link:
                continue
            if text.lower() == 'текст' and link.lower() == 'url':
                continue  # литеральная шапка таблицы

            rec = {
                'row': pos + 2,
                'sheet': sheet_name,
                'text': text,
                'url': link,
                'type': '' if (type_col is None or pd.isna(type_col.iloc[pos]))
                        else str(type_col.iloc[pos]).strip(),
                'author': '' if (author_col is None or pd.isna(author_col.iloc[pos]))
                          else str(author_col.iloc[pos]).strip(),
                'status': None, 'match_type': None, 'snippet': None, 'error': None,
            }
            records.append(rec)
    return records


def process_excel_file(filename, sheet_names=None, csv_output=None, timeout=10,
                       workers=8, retries=3, rate=6.0, limit=None, delay=0.0):
    """
    Обрабатывает Excel-файл: параллельно проверяет ссылки, пишет полный CSV-отчёт.

    Args:
        filename: путь к Excel-файлу
        sheet_names: листы (None — все; 0 — первый; список имён/индексов)
        csv_output: путь к CSV (None — авто <input>_results_<timestamp>.csv)
        timeout: таймаут HTTP-запроса
        workers: число параллельных потоков
        retries: доп. попыток при троттлинге/сбое
        rate: целевая частота запросов (req/s) для глобального лимитера
        limit: обработать только первые N строк-сообщений (валидация)
        delay: верхняя граница джиттера между стартами запросов (сек)
    """
    logger.info(f"Starting processing file: {filename}")
    try:
        logger.info("Reading Excel file...")
        if sheet_names is None:
            sheets_dict = pd.read_excel(filename, sheet_name=None)
        else:
            sheets_dict = pd.read_excel(filename, sheet_name=sheet_names)
            if not isinstance(sheets_dict, dict):
                key = sheet_names[0] if isinstance(sheet_names, list) else sheet_names
                sheets_dict = {key: sheets_dict}
        logger.info(f"Sheets to scan: {len(sheets_dict)}")

        records = _build_records(sheets_dict)
        if limit is not None:
            records = records[:limit]
        total = len(records)
        logger.info(f"Message rows to process: {total}")
        if total == 0:
            logger.warning("No message rows found - nothing to do")
            return

        jitter = (0.0, delay if delay and delay > 0 else 0.3)
        rate_limiter = RateLimiter(rate, jitter=jitter)

        # Основной проход
        _run_pass(records, workers, timeout, retries, rate_limiter, 'pass1')

        # Второй, медленный проход — ТОЛЬКО по транзиентным ошибкам (таймаут/429/
        # соединение). Приватные /c/-каналы и стабы детерминированно непроверяемы —
        # их повтор бессмыслен и лишь тратит время.
        err_recs = [r for r in records
                    if r['status'] == STATUS_ERROR and _is_transient_error(r.get('error'))]
        if err_recs:
            logger.info(f"Second slow pass over {len(err_recs)} transient {STATUS_ERROR} rows...")
            slow_limiter = RateLimiter(1.0, jitter=(0.0, 0.3))
            _run_pass(err_recs, min(2, workers), timeout, max(retries, 3),
                      slow_limiter, 'pass2')

        # Стабильный порядок: по листу и Excel-строке
        records.sort(key=lambda r: (str(r.get('sheet', '')), r['row']))

        if csv_output is None:
            base = os.path.splitext(filename)[0]
            csv_output = f"{base}_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        write_results_csv(records, csv_output)

        alive = sum(1 for r in records if r['status'] == STATUS_ALIVE)
        deleted = sum(1 for r in records if r['status'] == STATUS_DELETED)
        errors = sum(1 for r in records if r['status'] == STATUS_ERROR)
        logger.info(f"\n{'='*80}")
        logger.info("FINAL STATISTICS:")
        logger.info(f"Total message rows: {total}")
        logger.info(f"{STATUS_ALIVE}: {alive}")
        logger.info(f"{STATUS_DELETED}: {deleted}")
        logger.info(f"{STATUS_ERROR}: {errors}")
        logger.info(f"Results CSV: {csv_output}")
        logger.info(f"Log file: {log_filename}")

        print(f"\nВсего строк-сообщений: {total}")
        print(f"  {STATUS_ALIVE}:    {alive}")
        print(f"  {STATUS_DELETED}: {deleted}")
        print(f"  {STATUS_ERROR}:  {errors}")
        print(f"CSV-отчёт: {csv_output}")

    except FileNotFoundError:
        logger.error(f"File {filename} not found")
    except Exception as e:
        logger.error(f"Critical error processing file: {str(e)}")
        raise


def check_single_url(url, text, timeout=10, retries=3, csv_output=None):
    """
    Проверяет наличие текста на одной веб-странице.

    Args:
        url: URL для проверки
        text: Текст для поиска
        timeout: таймаут HTTP-запроса
        retries: число доп. попыток при троттлинге/сбое
        csv_output: Путь к CSV файлу для сохранения результата (опционально)

    Returns:
        bool: True, если статус ЖИВ.
    """
    logger.info("="*80)
    logger.info("Single URL check mode")
    logger.info(f"URL: {url}")
    logger.info(f"Text to search: {text[:100]}{'...' if len(text) > 100 else ''}")
    logger.info(f"Telegram host: {is_telegram_host(url)}")
    logger.info("="*80)

    status, error, match_type, snippet = check_text_on_page(
        url, text, timeout=timeout, retries=retries)

    logger.info(f"STATUS: {status} (match_type={match_type}, error={error})")
    if snippet:
        logger.info(f"Snippet: {snippet[:300]}")

    if status == STATUS_ALIVE:
        print(f"\n✓ {STATUS_ALIVE} ({match_type}) — текст найден на странице")
        if snippet:
            print(f"  Найдено: {snippet[:300]}")
    elif status == STATUS_DELETED:
        print(f"\n✗ {STATUS_DELETED} — текст отсутствует на странице")
    else:
        print(f"\n⚠ {STATUS_ERROR} — {error}")

    if csv_output and status == STATUS_ALIVE:
        record = {'row': '', 'type': '', 'author': '', 'url': url, 'text': text,
                  'status': status, 'match_type': match_type, 'snippet': snippet}
        if write_results_csv([record], csv_output):
            print(f"Result saved to CSV: {csv_output}")

    return status == STATUS_ALIVE

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
        default=0.3,
        help='Upper bound of random jitter between request starts, seconds (default: 0.3)'
    )

    parser.add_argument(
        '-t', '--timeout',
        type=int,
        default=10,
        help='Timeout for HTTP requests in seconds (default: 10)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=8,
        help='Number of parallel worker threads (default: 8)'
    )

    parser.add_argument(
        '--retries',
        type=int,
        default=3,
        help='Extra attempts on throttling/network errors (default: 3)'
    )

    parser.add_argument(
        '--rate',
        type=float,
        default=6.0,
        help='Global target request rate, req/s, for the shared limiter (default: 6.0)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Process only the first N message rows (for validation runs)'
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
        success = check_single_url(args.url, args.text, timeout=args.timeout,
                                   retries=args.retries, csv_output=csv_output)

        print(f"\nLog saved to: {log_filename}")
        sys.exit(0 if success else 1)

    # Режим обработки Excel файла
    # csv_output=None → process_excel_file сам сгенерирует <input>_results_<timestamp>.csv
    csv_output = args.output_csv

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
    print(f"CSV output: {csv_output if csv_output else '<input>_results_<timestamp>.csv (auto)'}")
    print(f"Workers: {args.workers} | Rate: {args.rate} req/s | Retries: {args.retries} | Timeout: {args.timeout}s")
    if args.limit:
        print(f"Limit: first {args.limit} message rows (validation run)")
    print("="*80)
    print()

    # Запуск обработки
    process_excel_file(
        args.excel_file, sheet_names=sheet_names, csv_output=csv_output,
        timeout=args.timeout, workers=args.workers, retries=args.retries,
        rate=args.rate, limit=args.limit, delay=args.delay)

    print()
    print("="*80)
    print("Processing completed!")
    print(f"Detailed results in file: {log_filename}")
    print("="*80)

if __name__ == "__main__":
    main()


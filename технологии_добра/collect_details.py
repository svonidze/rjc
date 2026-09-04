#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сбора подробной информации с сайтов участников
"""

import json
import time
import re
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin


def normalize_url(url: str) -> Optional[str]:
    """
    Нормализовать URL
    """
    if not url:
        return None
    
    url = url.strip()
    
    # Убираем кириллические домены если они некорректны
    if url.startswith('заботадетям.рф'):
        return None
    
    # Добавляем http:// если нет протокола
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    
    return url


def extract_contacts(soup: BeautifulSoup, url: str) -> Dict:
    """
    Извлечь контактную информацию с сайта
    """
    contacts = {
        'emails': [],
        'phones': [],
        'social_links': [],
        'address': None
    }
    
    # Извлекаем текст со страницы
    text = soup.get_text()
    
    # Ищем email
    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    contacts['emails'] = list(set(emails))[:5]  # Берем первые 5 уникальных
    
    # Ищем телефоны
    phones = re.findall(r'\+?[78][\s-]?\(?[0-9]{3}\)?[\s-]?[0-9]{3}[\s-]?[0-9]{2}[\s-]?[0-9]{2}', text)
    contacts['phones'] = list(set(phones))[:5]  # Берем первые 5 уникальных
    
    # Ищем социальные сети
    social_patterns = {
        'vk': r'vk\.com/[\w-]+',
        'telegram': r't\.me/[\w-]+',
        'facebook': r'facebook\.com/[\w-]+',
        'instagram': r'instagram\.com/[\w-]+',
        'youtube': r'youtube\.com/[\w-]+',
    }
    
    for network, pattern in social_patterns.items():
        links = re.findall(pattern, text, re.IGNORECASE)
        for link in links[:2]:  # Берем первые 2
            if not link.startswith('http'):
                link = 'https://' + link
            contacts['social_links'].append({
                'network': network,
                'url': link
            })
    
    return contacts


def extract_description(soup: BeautifulSoup) -> Optional[str]:
    """
    Извлечь описание деятельности организации
    """
    # Ищем мета-описание
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    if meta_desc and meta_desc.get('content'):
        return meta_desc['content'].strip()
    
    # Ищем OG описание
    og_desc = soup.find('meta', attrs={'property': 'og:description'})
    if og_desc and og_desc.get('content'):
        return og_desc['content'].strip()
    
    # Ищем первый абзац с классом о нас / о фонде
    about_section = soup.find(['div', 'section'], class_=re.compile(r'about|о-нас|o-nas', re.IGNORECASE))
    if about_section:
        p = about_section.find('p')
        if p:
            text = p.get_text().strip()
            if len(text) > 50:
                return text[:500]  # Первые 500 символов
    
    # Ищем первый длинный абзац
    paragraphs = soup.find_all('p')
    for p in paragraphs:
        text = p.get_text().strip()
        if len(text) > 100:
            return text[:500]  # Первые 500 символов
    
    return None


def scrape_website(url: str) -> Dict:
    """
    Собрать информацию с сайта
    """
    result = {
        'url': url,
        'success': False,
        'error': None,
        'contacts': {},
        'description': None,
        'title': None
    }
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Извлекаем title
        title_tag = soup.find('title')
        if title_tag:
            result['title'] = title_tag.get_text().strip()
        
        # Извлекаем контакты
        result['contacts'] = extract_contacts(soup, url)
        
        # Извлекаем описание
        result['description'] = extract_description(soup)
        
        result['success'] = True
        
    except requests.exceptions.Timeout:
        result['error'] = 'Timeout'
    except requests.exceptions.ConnectionError:
        result['error'] = 'Connection error'
    except requests.exceptions.HTTPError as e:
        result['error'] = f'HTTP error: {e.response.status_code}'
    except Exception as e:
        result['error'] = str(e)[:100]
    
    return result


def collect_details(participants_file: str = "participants.json", 
                    output_file: str = "participants_detailed.json",
                    limit: Optional[int] = None):
    """
    Собрать детальную информацию об участниках
    """
    # Загружаем участников
    with open(participants_file, 'r', encoding='utf-8') as f:
        participants = json.load(f)
    
    print(f"Загружено {len(participants)} участников")
    
    # Фильтруем участников с сайтами
    participants_with_sites = [p for p in participants if p.get('site')]
    print(f"Участников с сайтами: {len(participants_with_sites)}")
    
    if limit:
        participants_with_sites = participants_with_sites[:limit]
        print(f"Ограничение до {limit} участников для теста")
    
    # Собираем информацию
    detailed_data = []
    
    for i, participant in enumerate(participants_with_sites, 1):
        print(f"\n[{i}/{len(participants_with_sites)}] {participant.get('shortName', 'Без названия')}")
        
        url = normalize_url(participant.get('site'))
        if not url:
            print(f"  ⚠️  Некорректный URL: {participant.get('site')}")
            detailed_data.append({
                **participant,
                'details': {'error': 'Invalid URL'}
            })
            continue
        
        print(f"  🌐 {url}")
        
        # Собираем данные с сайта
        details = scrape_website(url)
        
        if details['success']:
            print(f"  ✅ Успешно собрано")
            if details['contacts']['emails']:
                print(f"     📧 Email: {', '.join(details['contacts']['emails'][:2])}")
            if details['contacts']['phones']:
                print(f"     📞 Телефон: {', '.join(details['contacts']['phones'][:2])}")
            if details['description']:
                print(f"     📝 Описание: {details['description'][:80]}...")
        else:
            print(f"  ❌ Ошибка: {details['error']}")
        
        detailed_data.append({
            **participant,
            'details': details
        })
        
        # Сохраняем промежуточные результаты каждые 10 участников
        if i % 10 == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(detailed_data, f, ensure_ascii=False, indent=2)
            print(f"\n  💾 Промежуточное сохранение ({i} участников)")
        
        # Небольшая задержка между запросами
        time.sleep(1)
    
    # Финальное сохранение
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(detailed_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*80}")
    print("ГОТОВО!")
    print(f"{'='*80}")
    print(f"Обработано участников: {len(detailed_data)}")
    
    successful = sum(1 for d in detailed_data if d.get('details', {}).get('success'))
    print(f"Успешно собрано: {successful}")
    print(f"С ошибками: {len(detailed_data) - successful}")
    print(f"\nДанные сохранены в {output_file}")


def main():
    """
    Основная функция
    """
    import sys
    
    # Можно указать лимит для тестирования: python collect_details.py 10
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"Режим тестирования: обработка первых {limit} участников")
        except ValueError:
            print("Неверный формат лимита, обрабатываю всех")
    
    collect_details(limit=limit)


if __name__ == "__main__":
    main()


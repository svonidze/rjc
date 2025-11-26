#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сбора информации об участниках проекта "Технологии добра"
"""

import requests
import json
import time
from typing import List, Dict, Optional
import csv

BASE_URL = "https://xn--80abfedtavrkbe1aq0c.xn--p1ai/api/v1/public/organizations"


def get_all_participants() -> List[Dict]:
    """
    Получить всех участников через API
    """
    all_participants = []
    page = 1
    per_page = 100  # Увеличим для быстрой загрузки
    
    print("Начинаю загрузку участников через API...")
    
    while True:
        url = f"{BASE_URL}?page={page}&perPage={per_page}"
        print(f"Загружаю страницу {page}...")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != 'Success':
                print(f"Ошибка: {data.get('message')}")
                break
            
            participants = data.get('data', [])
            if not participants:
                print("Больше нет участников")
                break
            
            all_participants.extend(participants)
            print(f"  Загружено {len(participants)} участников на странице {page}")
            print(f"  Всего участников: {len(all_participants)}")
            
            # Проверяем, есть ли следующая страница
            meta = data.get('meta', {})
            if not meta.get('nextPageUrl'):
                print("Достигнута последняя страница")
                break
            
            page += 1
            time.sleep(0.5)  # Небольшая задержка между запросами
            
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при загрузке страницы {page}: {e}")
            break
    
    print(f"\nВсего загружено {len(all_participants)} участников")
    return all_participants


def save_participants_json(participants: List[Dict], filename: str = "participants.json"):
    """
    Сохранить участников в JSON файл
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(participants, f, ensure_ascii=False, indent=2)
    print(f"Данные сохранены в {filename}")


def save_participants_csv(participants: List[Dict], filename: str = "participants.csv"):
    """
    Сохранить участников в CSV файл
    """
    if not participants:
        print("Нет данных для сохранения в CSV")
        return
    
    # Определяем все возможные поля
    fieldnames = ['id', 'shortName', 'regionOfPresence', 'site', 'logoUrl']
    
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(participants)
    
    print(f"Данные сохранены в {filename}")


def main():
    """
    Основная функция
    """
    print("=" * 80)
    print("Сбор информации об участниках проекта 'Технологии добра'")
    print("=" * 80)
    print()
    
    # Шаг 1: Получить всех участников
    participants = get_all_participants()
    
    if not participants:
        print("Не удалось загрузить участников")
        return
    
    # Шаг 2: Сохранить в файлы
    print("\nСохраняю данные...")
    save_participants_json(participants)
    save_participants_csv(participants)
    
    # Статистика
    print("\n" + "=" * 80)
    print("СТАТИСТИКА")
    print("=" * 80)
    print(f"Всего участников: {len(participants)}")
    
    participants_with_sites = [p for p in participants if p.get('site')]
    print(f"Участников с сайтами: {len(participants_with_sites)}")
    
    participants_with_region = [p for p in participants if p.get('regionOfPresence')]
    print(f"Участников с указанным регионом: {len(participants_with_region)}")
    
    participants_with_logo = [p for p in participants if p.get('logoUrl')]
    print(f"Участников с логотипом: {len(participants_with_logo)}")
    
    print("\n" + "=" * 80)
    print("Готово!")
    print("=" * 80)


if __name__ == "__main__":
    main()


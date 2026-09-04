#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конвертация participants_detailed.json в CSV
"""

import json
import csv
from typing import List, Dict, Any


def flatten_participant(participant: Dict[str, Any]) -> Dict[str, str]:
    """
    Преобразовать вложенную структуру участника в плоский словарь для CSV
    """
    details = participant.get('details', {})
    contacts = details.get('contacts', {})
    
    # Собираем социальные сети
    social_links = contacts.get('social_links', [])
    social_networks = {}
    for link in social_links:
        network = link.get('network', 'social')
        url = link.get('url', '')
        # Группируем по типам соцсетей
        if network not in social_networks:
            social_networks[network] = []
        social_networks[network].append(url)
    
    # Формируем плоскую структуру
    flat = {
        'ID': participant.get('id', ''),
        'Название': participant.get('shortName', ''),
        'Регион': participant.get('regionOfPresence', ''),
        'Сайт': participant.get('site', ''),
        'Логотип URL': participant.get('logoUrl', ''),
        
        # Детали
        'Заголовок сайта': details.get('title', ''),
        'Описание': details.get('description', ''),
        
        # Контакты
        'Email 1': contacts.get('emails', [])[0] if len(contacts.get('emails', [])) > 0 else '',
        'Email 2': contacts.get('emails', [])[1] if len(contacts.get('emails', [])) > 1 else '',
        'Email 3': contacts.get('emails', [])[2] if len(contacts.get('emails', [])) > 2 else '',
        'Все Email': '; '.join(contacts.get('emails', [])),
        
        'Телефон 1': contacts.get('phones', [])[0] if len(contacts.get('phones', [])) > 0 else '',
        'Телефон 2': contacts.get('phones', [])[1] if len(contacts.get('phones', [])) > 1 else '',
        'Телефон 3': contacts.get('phones', [])[2] if len(contacts.get('phones', [])) > 2 else '',
        'Все телефоны': '; '.join(contacts.get('phones', [])),
        
        # Социальные сети
        'VK': '; '.join(social_networks.get('vk', [])),
        'Telegram': '; '.join(social_networks.get('telegram', [])),
        'Facebook': '; '.join(social_networks.get('facebook', [])),
        'Instagram': '; '.join(social_networks.get('instagram', [])),
        'YouTube': '; '.join(social_networks.get('youtube', [])),
        
        # Статус обработки
        'Статус': 'Успешно' if details.get('success') else ('Ошибка' if details else 'Не обработано'),
        'Ошибка': details.get('error', '') if not details.get('success') else '',
        
        # URL обработанный
        'URL обработанный': details.get('url', ''),
    }
    
    return flat


def convert_json_to_csv(
    json_file: str = 'participants_detailed.json',
    csv_file: str = 'participants_detailed.csv'
):
    """
    Конвертировать JSON в CSV
    """
    print(f"Загружаю данные из {json_file}...")
    
    # Загружаем JSON
    with open(json_file, 'r', encoding='utf-8') as f:
        participants = json.load(f)
    
    print(f"Загружено {len(participants)} участников")
    
    # Преобразуем в плоские записи
    print("Преобразую данные...")
    flat_data = [flatten_participant(p) for p in participants]
    
    if not flat_data:
        print("Нет данных для экспорта!")
        return
    
    # Определяем порядок колонок
    fieldnames = [
        'ID', 'Название', 'Регион', 'Сайт',
        'Email 1', 'Email 2', 'Email 3', 'Все Email',
        'Телефон 1', 'Телефон 2', 'Телефон 3', 'Все телефоны',
        'Описание', 'Заголовок сайта',
        'VK', 'Telegram', 'Facebook', 'Instagram', 'YouTube',
        'Логотип URL', 'URL обработанный',
        'Статус', 'Ошибка'
    ]
    
    # Записываем в CSV
    print(f"Сохраняю в {csv_file}...")
    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(flat_data)
    
    print(f"\n✅ Готово!")
    print(f"Файл сохранен: {csv_file}")
    
    # Статистика
    print(f"\n{'='*80}")
    print("СТАТИСТИКА")
    print(f"{'='*80}")
    print(f"Всего записей: {len(flat_data)}")
    
    with_email = sum(1 for d in flat_data if d['Все Email'])
    print(f"С email: {with_email} ({with_email/len(flat_data)*100:.1f}%)")
    
    with_phone = sum(1 for d in flat_data if d['Все телефоны'])
    print(f"С телефоном: {with_phone} ({with_phone/len(flat_data)*100:.1f}%)")
    
    with_description = sum(1 for d in flat_data if d['Описание'])
    print(f"С описанием: {with_description} ({with_description/len(flat_data)*100:.1f}%)")
    
    successful = sum(1 for d in flat_data if d['Статус'] == 'Успешно')
    print(f"Успешно обработано: {successful} ({successful/len(flat_data)*100:.1f}%)")
    
    with_vk = sum(1 for d in flat_data if d['VK'])
    print(f"С VK: {with_vk}")
    
    with_telegram = sum(1 for d in flat_data if d['Telegram'])
    print(f"С Telegram: {with_telegram}")


def main():
    """
    Основная функция
    """
    convert_json_to_csv()


if __name__ == "__main__":
    main()


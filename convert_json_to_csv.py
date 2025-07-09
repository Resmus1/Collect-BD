import os
import json
import csv

def check_data(item, name):
    value = item.get(name)
    if isinstance(value, list) and value:
        return ', '.join(map(str, value))
    elif isinstance(value, str) and value.strip():
        return value
    else:
        return '-'

input_base = 'output'
output_base = 'output_csv'
os.makedirs(output_base, exist_ok=True)

fieldnames = [
    'Имя', 'Рейтинг', 'Кол-во отзывов', 'Телефоны', 'Сайт', 'WhatsApp', 'Telegram',
    'VK', 'OK', 'Instagram', 'Twitter', 'Facebook', 'Youtube', 'Email', 'Адрес'
]

for root, dirs, files in os.walk(input_base):
    for file in files:
        if file.endswith('.json'):
            json_path = os.path.join(root, file)
            csv_name = os.path.splitext(file)[0] + '.csv'
            csv_path = os.path.join(output_base, csv_name)

            with open(json_path, 'r', encoding='utf-8') as f_json:
                try:
                    data = json.load(f_json)
                except json.JSONDecodeError:
                    print(f"⚠️ Проблема с файлом: {json_path}")
                    continue

            with open(csv_path, 'w', newline='', encoding='utf-8') as f_csv:
                writer = csv.DictWriter(f_csv, fieldnames=fieldnames, delimiter=';')
                writer.writeheader()

                for item in data:
                    socials = item.get('socials', {})
                    row = {
                        'Имя': item.get('name'),
                        'Рейтинг': item.get('rating') or '0',
                        'Кол-во отзывов': item.get('count_reviews') or '0',
                        'Телефоны': check_data(item, 'phones'),
                        'Адрес': check_data(item, 'address'),
                        'Email': check_data(item, 'email'),
                        'Сайт': check_data(item, 'website'),
                        'WhatsApp': check_data(socials, 'WhatsApp'),
                        'Telegram': check_data(socials, 'Telegram'),
                        'VK': check_data(socials, 'ВКонтакте'),
                        'OK': check_data(socials, 'Одноклассники'),
                        'Instagram': check_data(socials, 'Instagram'),
                        'Facebook': check_data(socials, 'Facebook'),
                        'Twitter': check_data(socials, 'Twitter'),
                        'Youtube': check_data(socials, ' YouTube'),
                    }
                    writer.writerow(row)

            print(f'✅ Обработан: {json_path} → {csv_path}')

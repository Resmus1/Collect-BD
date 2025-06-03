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


with open('output.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

with open('output.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = [
        'Имя', 'Рейтинг', 'Кол-во отзывов', 'Телефоны', 'Сайт', 'WhatsApp', 'Telegram',
        'VK', 'OK', 'Instagram', 'Twitter', 'Facebook', 'Youtube', 'Email', 'Адрес'
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()

    for item in data:
        row = {
            'Имя': item.get('name'),
            'Рейтинг': item.get('rating') or '0',
            'Кол-во отзывов': item.get('count_reviews') or '0',
            'Телефоны': check_data(item, 'phones'),
            'Адрес': check_data(item, 'address'),
            'Email': check_data(item, 'email'),
            'Сайт': check_data(item, 'website'),
            'WhatsApp': check_data(item['socials'], 'WhatsApp'),
            'Telegram': check_data(item['socials'], 'Telegram'),
            'VK': check_data(item['socials'], 'ВКонтакте'),
            'OK': check_data(item['socials'], 'Одноклассники'),
            'Instagram': check_data(item['socials'], 'Instagram'),
            'Facebook': check_data(item['socials'], 'Facebook'),
            'Twitter': check_data(item['socials'], 'Twitter'),
            'Youtube': check_data(item['socials'], ' YouTube'),
        }
        writer.writerow(row)

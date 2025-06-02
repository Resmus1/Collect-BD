import json
import csv

with open('output.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

with open('output.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = [
        'name', 'rating', 'count_reviews', 'address', 'WhatsApp', 'Telegram', 'Instagram', 'VK',
        'Facebook', 'Youtube', 'phones', 'email', 'website'
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()

    for item in data:
        row = {
            'name': item.get('name'),
            'rating': item.get('rating'),
            'count_reviews': item.get('count_reviews'),
            'phones': item.get('phones'),
            'address': item.get('address'),
            'email': item.get('email'),
            'website': item.get('website'),
            'WhatsApp': item['socials'].get('WhatsApp'),
            'Telegram': item['socials'].get('Telegram'),
            'Instagram': item['socials'].get('Instagram'),
            'VK': item['socials'].get('VK'),
            'Facebook': item['socials'].get('Facebook'),
            'Youtube': item['socials'].get('YouTube'),
        }
        writer.writerow(row)

import os
import json
import csv

def to_set(value):
    if isinstance(value, list):
        return set(str(v).strip() for v in value if v)
    elif isinstance(value, str) and value.strip():
        return {value.strip()}
    return set()

def check_social(item, name):
    return to_set(item.get('socials', {}).get(name))

input_base = 'last_data'
output_base = 'output_csv'
os.makedirs(output_base, exist_ok=True)

fieldnames = ['Имя', 'Регион', 'Категория', 'Email', 'Телефоны', 'Сайты', 'WhatsApp']
all_rows = []
total_items = 0

for region in os.listdir(input_base):
    region_path = os.path.join(input_base, region)
    if not os.path.isdir(region_path):
        continue

    for file_name in os.listdir(region_path):
        if not file_name.endswith('.json'):
            continue

        category = os.path.splitext(file_name)[0]  # имя файла без .json
        json_path = os.path.join(region_path, file_name)

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Ошибка чтения: {json_path}")
            continue

        valid_count = 0

        for item in data:
            name = item.get('name', '').strip()
            if not name:
                continue

            # Пропуск без сайта
            if not item.get('website') or not str(item.get('website')).strip():
                continue

            row = {
                'Имя': name,
                'Регион': region,
                'Категория': category,
                'Email': ', '.join(to_set(item.get('email'))),
                'Телефоны': ', '.join(to_set(item.get('phones'))),
                'Сайты': ', '.join(to_set(item.get('website'))),
                'WhatsApp': ', '.join(check_social(item, 'WhatsApp')),
            }

            all_rows.append(row)
            valid_count += 1

        print(f'✅ {region}/{file_name} | записей с сайтом: {valid_count}')
        total_items += valid_count

# Сохранение CSV
output_file = os.path.join(output_base, 'simple_firms.csv')
with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()
    writer.writerows(all_rows)

# Финальная статистика
print('\n📊 Итог:')
print(f'🔹 Всего записей: {len(all_rows)}')
print(f'📁 CSV сохранён: {output_file}')

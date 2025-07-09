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

all_rows = []
total_items = 0
total_valid = 0

fieldnames = [
    'Рубрика', 'Имя', 'Сайт', 'Телефоны', 'Email', 'WhatsApp'
]

for root, dirs, files in os.walk(input_base):
    for file in files:
        if file.endswith('.json'):
            json_path = os.path.join(root, file)
            rubric = os.path.splitext(file)[0]  # Название файла без расширения

            with open(json_path, 'r', encoding='utf-8') as f_json:
                try:
                    data = json.load(f_json)
                except json.JSONDecodeError:
                    print(f"⚠️ Проблема с файлом: {json_path}")
                    continue

            total_items += len(data)
            valid_count = 0

            for item in data:
                if not item.get('name') or not item.get('website'):
                    continue  # Пропуск без обязательных полей

                socials = item.get('socials', {})
                row = {
                    'Рубрика': rubric,
                    'Имя': item.get('name'),
                    'Сайт': check_data(item, 'website'),
                    'Телефоны': check_data(item, 'phones'),
                    'Email': check_data(item, 'email'),
                    'WhatsApp': check_data(socials, 'WhatsApp'),
                }
                all_rows.append(row)
                valid_count += 1

            total_valid += valid_count
            print(f'✅ {rubric} | всего: {len(data)}, записей в таблицу: {valid_count}')

# Запись в CSV
output_file = os.path.join(output_base, 'filtered_data.csv')
with open(output_file, 'w', newline='', encoding='utf-8') as f_csv:
    writer = csv.DictWriter(f_csv, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()
    writer.writerows(all_rows)

print('\n📊 Статистика:')
print(f'🔹 Всего записей во всех файлах: {total_items}')
print(f'🔹 Добавлено в таблицу: {total_valid}')
print(f'📁 CSV сохранён: {output_file}')

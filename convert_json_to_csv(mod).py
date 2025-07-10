import os
import json
import csv


def to_set(value):
    if isinstance(value, list):
        return set(str(v).strip() for v in value if v)
    elif isinstance(value, str) and value.strip():
        return {value.strip()}
    return set()


def check_data(item, name):
    return to_set(item.get(name))


def check_social(item, name):
    return to_set(item.get('socials', {}).get(name))


input_base = 'output'
output_base = 'output_csv'
os.makedirs(output_base, exist_ok=True)

fieldnames = [
    'Имя', 'Регион', 'Категории', 'Подкатегории',
    'Сайты', 'Телефоны', 'Email', 'WhatsApp'
]

firm_dict = {}
total_files = 0
total_items = 0

region_stats = {}  # {region: {'total': int, 'unique': set()}}

for region in os.listdir(input_base):
    region_path = os.path.join(input_base, region)
    if not os.path.isdir(region_path):
        continue

    for category in os.listdir(region_path):
        category_path = os.path.join(region_path, category)
        if not os.path.isdir(category_path):
            continue

        for sub_file in os.listdir(category_path):
            if not sub_file.endswith('.json'):
                continue

            total_files += 1
            subcategory = os.path.splitext(sub_file)[0]
            json_path = os.path.join(category_path, sub_file)

            try:
                with open(json_path, 'r', encoding='utf-8') as f_json:
                    data = json.load(f_json)
            except json.JSONDecodeError:
                print(f"⚠️ Проблема с файлом: {json_path}")
                continue

            total_items += len(data)
            valid_count = 0

            for item in data:
                name = item.get('name')
                if not name:
                    continue

                if not item.get('website') or not str(item.get('website')).strip():
                    continue  # ⛔️ Пропуск карточек без сайта

                name = name.strip()

                key = f"{region}__{name}"

                if key not in firm_dict:
                    firm_dict[key] = {
                        'Имя': name,
                        'Регион': region,
                        'Категории': set(),
                        'Подкатегории': set(),
                        'Сайты': set(),
                        'Телефоны': set(),
                        'Email': set(),
                        'WhatsApp': set()
                    }

                entry = firm_dict[key]
                entry['Категории'].add(category)
                entry['Подкатегории'].add(subcategory)
                entry['Сайты'].update(check_data(item, 'website'))
                entry['Телефоны'].update(check_data(item, 'phones'))
                entry['Email'].update(check_data(item, 'email'))
                entry['WhatsApp'].update(check_social(item, 'WhatsApp'))

                # Инициализация счётчиков для региона
                if region not in region_stats:
                    region_stats[region] = {'total': 0, 'unique_keys': set()}

                region_stats[region]['total'] += 1
                region_stats[region]['unique_keys'].add(key)

                valid_count += 1

            print(
                f'✅ {region}/{category}/{subcategory} | записей в таблицу: {valid_count}')

# Подготовка к записи
all_rows = []
for entry in firm_dict.values():
    row = {
        'Имя': entry['Имя'],
        'Регион': entry['Регион'],
        'Категории': ', '.join(sorted(entry['Категории'])),
        'Подкатегории': ', '.join(sorted(entry['Подкатегории'])),
        'Сайты': ', '.join(sorted(entry['Сайты'])),
        'Телефоны': ', '.join(sorted(entry['Телефоны'])),
        'Email': ', '.join(sorted(entry['Email'])),
        'WhatsApp': ', '.join(sorted(entry['WhatsApp'])),
    }
    all_rows.append(row)

# Запись в CSV
output_file = os.path.join(output_base, 'aggregated_with_website.csv')
with open(output_file, 'w', newline='', encoding='utf-8') as f_csv:
    writer = csv.DictWriter(f_csv, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()
    writer.writerows(all_rows)

# Статистика
print('\n📍 Статистика по регионам:')
for region, stats in region_stats.items():
    total = stats['total']
    unique = len(stats['unique_keys'])
    duplicates = total - unique
    print(f'🔸 {region}:')
    print(f'   ├─ Всего карточек: {total}')
    print(f'   ├─ Уникальных организаций: {unique}')
    print(f'   └─ Дубликатов по имени: {duplicates}')

print('\n📊 Общая статистика:')
print(f'🔹 Файлов обработано: {total_files}')
print(f'🔹 Всего записей во всех файлах: {total_items}')
print(f'🔹 Уникальных организаций по имени и региону: {len(all_rows)}')
print(f'📁 CSV сохранён: {output_file}')

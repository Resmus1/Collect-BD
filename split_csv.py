import csv
import os

# Папка с CSV-файлами
csv_folder = 'output_csv'
output_file = os.path.join(csv_folder, 'merged_firms.csv')

# Новые ожидаемые поля (с категорией)
fieldnames = ['Имя', 'Регион', 'Категория',
              'Email', 'Телефоны', 'Сайты', 'WhatsApp']

# Словарь уникальных организаций по ключу Имя+Регион (без категории)
unique_firms = {}

# Ищем все simple_firms*.csv в папке
csv_files = [f for f in os.listdir(csv_folder) if f.startswith(
    'simple_firms') and f.endswith('.csv')]

if len(csv_files) < 2:
    print('❌ Нужно как минимум два файла simple_firms*.csv для объединения.')
    exit()

for file_name in csv_files:
    file_path = os.path.join(csv_folder, file_name)
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        count = 0
        for row in reader:
            # Добавляем Категорию, если её нет в CSV
            row.setdefault('Категория', '')

            # Категорию не учитываем
            key = f"{row['Имя'].strip()}__{row['Регион'].strip()}"

            if key not in unique_firms:
                unique_firms[key] = row
                count += 1
        print(
            f"✅ Обработан файл: {file_name} | Уникальных строк добавлено: {count}")

# Сохраняем объединённый CSV
with open(output_file, 'w', newline='', encoding='utf-8-sig') as f_out:
    writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()
    writer.writerows(unique_firms.values())

print(f"\n📁 Итоговый файл сохранён: {output_file}")
print(f"🔹 Всего уникальных организаций: {len(unique_firms)}")

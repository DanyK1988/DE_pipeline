import os
import json
from pathlib import Path
from pymongo import MongoClient

# Мы находимся в папке scripts, нам надо подняться на уровень выше
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


# Подключаемся к MongoDB
client = MongoClient("mongodb://localhost:27017/?directConnection=true")
# Создаем Базу Данных
db = client['shop_database']

# Список папок плюс будем их использовать для названий коллекций
folders = ['customers', 'products', 'stores', 'purchases']

count = 0
for folder in folders:
    # Проверяем, существует ли папка, чтобы скрипт не сломался
    folder_path = os.path.join(DATA_DIR, folder)
    if not os.path.exists(folder_path):
        print(f"⚠️ Папка {folder} не найдена, пропускаю...")
        continue

    for file in os.listdir(folder_path):
        if file.endswith('.json'):  # Берем только JSON файлы
            file_path = os.path.join(folder_path, file)

            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    # Загружаем содержимое файла в переменную
                    data = json.load(f)

                    # Вставляем данные в коллекцию
                    db[folder].insert_one(data)
                    count += 1
                except Exception as e:
                    print(f"❌ Ошибка в файле {file}: {e}")

print(f'✅ Готово! База "shop_database" создана, залито документов: {count}')
import json
import time
import os
from pymongo import MongoClient
from kafka import KafkaProducer
from bson import json_util
import hashlib
import re


print("Ждем пока Kafka запустится...")
time.sleep(15)
# Читаем адреса из переменных окружения (указаны в docker-compose)
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo') # 'mongo' совпадает с именем сервиса


# 1. Настройка подключений
try:
    # Подключение к MongoDB
    mongo_client = MongoClient(f"mongodb://{MONGO_HOST}:27017")
    db = mongo_client['shop_database']

    # Подключение к Kafka
    # bootstrap_servers должен совпадать с адресом из docker-compose
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8')
    )
    print("✅ Подключение к Mongo и Kafka установлено")
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit()

# Список коллекций для миграции
collections = ['customers', 'products', 'stores', 'purchases']


def migrate_data():
    total_sent = 0

    for coll_name in collections:
        collection = db[coll_name]
        cursor = collection.find({})

        print(f"🚀 Начинаю отправку коллекции: {coll_name}")

        count = 0
        for doc in cursor:
            if coll_name == 'customers':
                doc['email'] = hashlib.md5(doc['email'].encode('utf-8')).hexdigest()
                raw_phone = doc.get('phone', '')

                phone_digits = re.sub(r"\D", "", str(raw_phone))
                if phone_digits.startswith("8"):
                    phone_digits = "7" + phone_digits[1:]
                if len(phone_digits) == 10:
                    phone_number = "7" + phone_digits
                doc['phone'] = hashlib.md5(phone_digits.encode('utf-8')).hexdigest()
            # Отправляем документ в топик с таким же именем
            # Kafka создаст топик автоматически при первой отправке (если включено в конфиге)
            producer.send(topic=coll_name, value=doc)
            count += 1
            total_sent += 1

        # Ждем завершения отправки текущей порции
        producer.flush()
        print(f"--- Завершено: {coll_name}. Отправлено документов: {count}")

    print(f"\n🎯 Миграция окончена! Всего сообщений в Kafka: {total_sent}")


if __name__ == "__main__":
    start_time = time.time()
    migrate_data()
    end_time = time.time()
    print(f"⏱ Время выполнения: {round(end_time - start_time, 2)} сек.")
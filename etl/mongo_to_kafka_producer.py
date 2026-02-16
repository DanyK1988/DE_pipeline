import json
import time
import os
import hashlib
import re
from pymongo import MongoClient
from kafka import KafkaProducer
from bson import json_util, ObjectId


print("⏳ Ждем пока Kafka запустится...")
time.sleep(15)

# ==============================
# 🔧 Конфигурация
# ==============================

KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo')

collections = ['customers', 'products', 'stores', 'purchases']


# ==============================
# 🔌 Подключения
# ==============================

try:
    mongo_client = MongoClient(f"mongodb://{MONGO_HOST}:27017")
    db = mongo_client['shop_database']
    metadata_collection = db['etl_metadata']

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(
            v, default=json_util.default
        ).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
        retries=5,
        linger_ms=5,
        enable_idempotence=True
    )

    print("✅ Подключение к Mongo и Kafka установлено")

except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit()


# ==============================
# 📱 Нормализация телефона
# ==============================

def normalize_phone(raw_phone: str) -> str:
    phone_digits = re.sub(r"\D", "", str(raw_phone))

    if phone_digits.startswith("8"):
        phone_digits = "7" + phone_digits[1:]

    if len(phone_digits) == 10:
        phone_digits = "7" + phone_digits

    return phone_digits


# ==============================
# 📦 Получение checkpoint
# ==============================

def get_last_processed_id(collection_name: str):
    record = metadata_collection.find_one({"collection": collection_name})
    if record:
        val = record.get("last_id")
        # Очень важно видеть в логах, что именно мы достали
        print(f"DEBUG: Checkpoint for {collection_name}: {val} (Type: {type(val)})")
        return val
    return None


def update_last_processed_id(collection_name: str, last_id):
    metadata_collection.update_one(
        {"collection": collection_name},
        {
            "$set": {
                "collection": collection_name,
                "last_id": last_id
            }
        },
        upsert=True
    )


# ==============================
# 🚀 Миграция
# ==============================

def migrate_data():
    total_sent = 0

    for coll_name in collections:
        collection = db[coll_name]
        last_id = get_last_processed_id(coll_name)

        if last_id:
            print(f"🔁 Продолжаем {coll_name} с _id > {last_id}")
            cursor = collection.find(
                {"_id": {"$gt": last_id}}
            ).sort("_id", 1)
        else:
            print(f"🚀 Первая загрузка коллекции: {coll_name}")
            cursor = collection.find({}).sort("_id", 1)

        count = 0
        last_processed = None

        for doc in cursor:

            # 🔐 Анонимизация customers
            if coll_name == 'customers':
                if 'email' in doc:
                    doc['email'] = hashlib.md5(
                        doc['email'].encode('utf-8')
                    ).hexdigest()

                if 'phone' in doc:
                    normalized = normalize_phone(doc['phone'])
                    doc['phone'] = hashlib.md5(
                        normalized.encode('utf-8')
                    ).hexdigest()

            # 📤 Отправка в Kafka
            producer.send(
                topic=coll_name,
                key=str(doc['_id']),
                value=doc
            )

            last_processed = doc['_id']
            count += 1
            total_sent += 1

        producer.flush()

        # 💾 Обновляем checkpoint
        if last_processed:
            update_last_processed_id(coll_name, last_processed)

        print(f"✔ Завершено: {coll_name}. Отправлено: {count}")

    print(f"\n🎯 Миграция завершена! Всего сообщений: {total_sent}")


# ==============================
# ▶ Запуск
# ==============================

if __name__ == "__main__":
    start_time = time.time()

    migrate_data()

    end_time = time.time()
    print(f"⏱ Время выполнения: {round(end_time - start_time, 2)} сек.")
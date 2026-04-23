# Прототип DE-пайплайна (Mongo → Kafka → ClickHouse → PySpark → Grafana)

Проект демонстрирует end-to-end контур: генерация тестовых сущностей → загрузка в MongoDB → публикация событий в Kafka → загрузка в ClickHouse (raw/silver) через Kafka-движок и Materialized View → аналитика в PySpark и визуализация в Grafana.

## Сервисы (docker-compose)
Определены в `docker-compose.yml`:
- **etl (`etl_pipeline`)**: контейнер с Python/Java/PySpark, читает Mongo и пишет в Kafka.
- **zookeeper**: координация для Kafka.
- **kafka**: брокер, топики соответствуют коллекциям: `customers`, `products`, `stores`, `purchases`.
- **mongo (`mongo_db`)**: источник данных, БД `shop_database`.
- **clickhouse (`clickhouse_final_task`)**: DWH, ingest из Kafka + raw/silver слои.
- **grafana**: BI поверх ClickHouse.
- **postgres**: поднят в составе окружения (в текущей версии кода ETL не обязателен).

Порты:
- **Kafka**: `9092`
- **MongoDB**: `27017`
- **ClickHouse HTTP**: `9123` (проброшен с `8123`)
- **ClickHouse native**: `10000` (проброшен с `9000`)
- **Grafana**: `3000`
- **Postgres**: `5432` (user/pass/db: `admin`/`admin`/`test_db`)

## Структура репозитория
```
.
├─ etl/
│  ├─ main.py                      # точка входа ETL контейнера: вызывает migrate_data()
│  ├─ mongo_to_kafka_producer.py   # Mongo → Kafka + анонимизация + checkpoint
│  └─ sql_scripts/
│     ├─ create_tables.sql         # ClickHouse raw + Kafka-очереди + MV ingest
│     └─ create_tables_silver.sql  # ClickHouse silver + MV очистки/валидаторы
├─ scripts/
│  └─ mongo_producer.py            # грузит JSON из data/* в Mongo (подключение к localhost)
├─ data/                           # сюда генерируются JSON (customers/products/stores/purchases)
├─ entity_generator.py             # генератор JSON сущностей в data/*
├─ ch_pyspark.ipynb                # пример чтения silver слоя в PySpark через JDBC
├─ docker-compose.yml              # инфраструктура
├─ Dockerfile                      # образ ETL (Python + Java для Spark)
└─ requirements.txt                # зависимости ETL контейнера
```

## Как это работает (поток данных)
1) **Генерация данных** (`entity_generator.py`)
- Создаёт JSON-документы и кладёт их в `data/{customers,products,stores,purchases}/`.

2) **Загрузка в MongoDB** (`scripts/mongo_producer.py`)
- Читает JSON-файлы из `data/*` и вставляет документы в Mongo в БД `shop_database`.

3) **ETL (Mongo → Kafka)** (`etl/mongo_to_kafka_producer.py`)
- Читает коллекции: `customers`, `products`, `stores`, `purchases`.
- Для `customers` делает:
  - **нормализацию телефона** (оставляет только цифры и приводит к `7...`);
  - **анонимизацию** `email` и `phone` через **MD5**.
- Публикует документы в Kafka в одноимённые топики.
- В Mongo хранит checkpoint по каждой коллекции в `etl_metadata` (последний обработанный `_id`), чтобы при повторном запуске догружать только новые документы.

4) **Ingest в ClickHouse без отдельного consumer**
- В `etl/sql_scripts/create_tables.sql` создаются:
  - таблицы `kafka_*_queue` с `ENGINE=Kafka` (они “слушают” Kafka топики),
  - Materialized Views `mv_*_etl`, которые парсят JSON (`JSONExtract*`) и пишут в **raw** таблицы на `MergeTree`.
- В `etl/sql_scripts/create_tables_silver.sql` создаются:
  - БД `silver` и таблицы на `ReplacingMergeTree`,
  - MV `mv_*_clean`, которые нормализуют строки (trim/lower) и применяют базовые проверки качества данных (непустые ключи, адекватные даты/цены) и пишут в **silver** слой.

## Запуск (с нуля)
### 0) Поднять контейнеры

```bash
docker compose up -d --build
```

### 1) Сгенерировать тестовые данные (на хосте)
Требуется Python 3 и пакет `faker` (генератор использует `Faker("ru_RU")`).

```bash
python3 -m pip install --upgrade pip
python3 -m pip install faker
python3 entity_generator.py
```

### 2) Загрузить JSON в Mongo (на хосте)
Скрипт подключается к Mongo по `localhost:27017`, который проброшен из контейнера `mongo`.

```bash
python3 -m pip install pymongo
python3 scripts/mongo_producer.py
```

### 3) Создать схему в ClickHouse (raw + silver)
Можно применить SQL прямо в контейнер ClickHouse.

```bash
docker exec -i clickhouse_final_task clickhouse-client \
  -u user --password strongpassword < etl/sql_scripts/create_tables.sql

docker exec -i clickhouse_final_task clickhouse-client \
  -u user --password strongpassword < etl/sql_scripts/create_tables_silver.sql
```

### 4) Запустить ETL (Mongo → Kafka)
Ручной запуск внутри ETL контейнера:

```bash
docker exec -it etl_pipeline python /app/etl/mongo_to_kafka_producer.py
```

После этого данные должны появиться в ClickHouse (raw и затем silver через MV).

## Проверка результата
- **ClickHouse** (пример):
  - `SELECT count() FROM default.raw_purchases;`
  - `SELECT count() FROM silver.purchases;`
- **Grafana**: `http://localhost:3000` (логин/пароль: `admin` / `admin`), источник данных — ClickHouse на `http://clickhouse:8123` внутри сети docker-compose или `http://localhost:9123` с хоста.

## Примечания
- **PII**: `customers.email` и `customers.phone` анонимизируются через MD5 (см. `etl/mongo_to_kafka_producer.py`).
- **PySpark + ClickHouse**: в `ch_pyspark.ipynb` показано чтение `silver.*` через JDBC. Для колонок `Array`/`Tuple` (например, `stores.categories`, `stores.location_coordinates`) используется обход через `SELECT` (склейка `categories` в строку и исключение координат).

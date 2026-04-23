# Прототип DE-пайплайна (Mongo → Kafka → ClickHouse → PySpark → BI/экспорт)

Проект демонстрирует полный контур от генерации сырых сущностей до загрузки в MongoDB, стриминга через Kafka и витрины в ClickHouse (raw/silver), с оркестрацией через Airflow и последующей аналитикой в PySpark.

## Стек и сервисы
- **Airflow (CeleryExecutor + Redis + Postgres)**: оркестрация, DAG запускает ETL как Docker-контейнер.
- **MongoDB**: источник (операционные коллекции).
- **Kafka + Zookeeper**: транспорт событий (топики по коллекциям).
- **ClickHouse**: DWH, ingest из Kafka через `ENGINE=Kafka` + Materialized Views, слой **raw** и слой **silver**.
- **Grafana**: BI поверх ClickHouse.
- **ETL контейнер (Python 3.11 + PySpark + Java)**: читает Mongo, анонимизирует PII и пишет в Kafka.

## Быстрый старт
Поднять инфраструктуру:

```bash
docker compose up -d --build
```

Открыть UI:
- **Airflow**: `http://localhost:9080` (логин/пароль по умолчанию: `airflow` / `airflow`)
- **ClickHouse HTTP**: `http://localhost:9123` (порт проброшен с 8123)
- **ClickHouse native**: `localhost:10000` (порт проброшен с 9000)
- **Grafana**: `http://localhost:3000` (логин/пароль по умолчанию: `admin` / `admin`)
- **Traefik dashboard**: `http://localhost:8082`

## Структура репозитория
```
.
├─ dags/
│  └─ run_etl_dag.py               # DAG Airflow: DockerOperator запускает ETL контейнер
├─ etl/
│  ├─ main.py                      # точка входа: вызывает migrate_data()
│  ├─ mongo_to_kafka_producer.py   # Mongo → Kafka + анонимизация + checkpoint
│  └─ sql_scripts/
│     ├─ create_tables.sql         # ClickHouse raw + kafka queues + MV raw ingest
│     └─ create_tables_silver.sql  # ClickHouse silver + MV очистки/валидаторы
├─ scripts/
│  └─ mongo_producer.py            # (локально) грузит JSON из data/* в Mongo
├─ entity_generator.py             # генератор JSON сущностей в data/*
├─ ch_pyspark.ipynb                # пример чтения silver слоя в PySpark через JDBC
├─ docker-compose.yml              # вся инфраструктура (Airflow/Kafka/Mongo/CH/Grafana/ETL)
├─ Dockerfile                      # образ ETL (Python + Java для Spark)
└─ requirements.txt                # зависимости ETL контейнера
```

## Как это работает (поток данных)
- **Генерация данных**: `entity_generator.py` создаёт JSON-документы и кладёт их в `data/{customers,products,stores,purchases}/`.
- **Загрузка в MongoDB**: `scripts/mongo_producer.py` читает JSON и вставляет документы в Mongo в БД `shop_database`.
- **ETL (Mongo → Kafka)**: `etl/mongo_to_kafka_producer.py`:
  - читает коллекции `customers/products/stores/purchases`;
  - для `customers` **нормализует телефон** и **хэширует MD5** поля `email` и `phone`;
  - пишет документы в Kafka в одноимённые топики;
  - хранит checkpoint `_id` по каждой коллекции в Mongo (`etl_metadata`), чтобы догружать инкрементально.
- **ClickHouse ingest без отдельного consumer**:
  - в `etl/sql_scripts/create_tables.sql` создаются таблицы `kafka_*_queue` с `ENGINE=Kafka`;
  - Materialized Views `mv_*_etl` парсят JSON (`JSONExtract*`) и кладут данные в таблицы **raw** (`ENGINE=MergeTree`).
- **Silver слой (очистка/валидация)**:
  - в `etl/sql_scripts/create_tables_silver.sql` создаётся БД `silver` и таблицы на `ReplacingMergeTree`;
  - MV `mv_*_clean` применяют простые правила качества (непустые ключи, адекватные даты/цены, trim/lower и т.п.) и пишут в **silver**.
- **Аналитика в PySpark**: ноутбук `ch_pyspark.ipynb` показывает чтение таблиц `silver.*` через JDBC.

## Как запустить пайплайн
### 1) Сгенерировать тестовые данные (JSON)

```bash
python3 entity_generator.py
```

### 2) Залить данные в MongoDB (локально)
Скрипт рассчитан на подключение к Mongo по `localhost:27017`:

```bash
python3 scripts/mongo_producer.py
```

### 3) Создать таблицы в ClickHouse
SQL лежит в `etl/sql_scripts/`:
- `create_tables.sql` — raw + kafka queues + MV ingest
- `create_tables_silver.sql` — silver + MV очистки

Примените их в ClickHouse (любой удобный способ: client/IDE).

### 4) Запустить ETL вручную (в контейнере)

```bash
docker exec -it etl_pipeline python /app/etl/mongo_to_kafka_producer.py
```

### 5) Запуск по расписанию через Airflow
DAG: `dags/run_etl_dag.py`, `dag_id=docker_etl_job`, расписание `@daily`.

Важно: в DAG указан образ `my_etl_image:latest`. Чтобы DAG реально стартовал ETL, образ должен существовать в Docker daemon:

```bash
docker build -t my_etl_image:latest .
```

После этого в Airflow можно включить DAG и запустить вручную/по расписанию.

## Где смотреть логи
- **Airflow task logs**: монтируются в `./logs/` (см. `docker-compose.yml`).
- **ETL логи**: вывод контейнера (в Airflow — внутри логов таски `execute_etl`, при ручном запуске — в терминале).

## Примечания и ограничения
- **PII**: `customers.email` и `customers.phone` анонимизируются через MD5 (см. `etl/mongo_to_kafka_producer.py`).
- **PySpark + ClickHouse**: Spark не всегда удобно читает `Array`/`Tuple` из ClickHouse; в `ch_pyspark.ipynb` показан обход (замена `stores` на `SELECT` с `arrayStringConcat`, координаты исключаются).

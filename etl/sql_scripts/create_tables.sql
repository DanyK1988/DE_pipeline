-- Данныый скрипты создают таблицы в Clickhouse
-- Алгоритм следующий, мы создаем слушателя kafka с движком kafka. Это избавляет нас от создания consumer скрипта
-- Слушатель получает данные с kafka и эти данные подхватывает Materilized View и распаршивает json документы и отправлет в соответствущие поля основной raw таблицы
-- Слушатель автоматически удаляет данные, а raw таблицы хранит данные всегда

-- Таблица продуктов
CREATE TABLE raw_products (
    id String,
    name String,
    group String,
    description String,
    kbju_calories Float32,
    kbju_protein Float32,
    kbju_fat Float32,
    kbju_carbohydrates Float32,
    price Float64,
    unit String,
    origin_country String,
    expiry_days Int16,
    is_organic Int8,
    barcode String,
    manufacturer_name String,
    manufacturer_inn String,
    load_datetime DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (group, id);

-- Очередь Kafka
CREATE TABLE kafka_products_queue (
    raw_payload String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'products',
         kafka_group_name = 'ch_products_group',
         kafka_format = 'JSONAsString';

-- Materialized View (Логика парсинга)
CREATE MATERIALIZED VIEW mv_products_etl TO raw_products AS
SELECT
    JSONExtractString(raw_payload, 'id') as id,
    JSONExtractString(raw_payload, 'name') as name,
    JSONExtractString(raw_payload, 'group') as group,
    JSONExtractString(raw_payload, 'description') as description,
    JSONExtractFloat(raw_payload, 'kbju', 'calories') as kbju_calories,
    JSONExtractFloat(raw_payload, 'kbju', 'protein') as kbju_protein,
    JSONExtractFloat(raw_payload, 'kbju', 'fat') as kbju_fat,
    JSONExtractFloat(raw_payload, 'kbju', 'carbohydrates') as kbju_carbohydrates,
    JSONExtractFloat(raw_payload, 'price') as price,
    JSONExtractString(raw_payload, 'unit') as unit,
    JSONExtractString(raw_payload, 'origin_country') as origin_country,
    JSONExtractInt(raw_payload, 'expiry_days') as expiry_days,
    JSONExtractBool(raw_payload, 'is_organic') as is_organic,
    JSONExtractString(raw_payload, 'barcode') as barcode,
    JSONExtractString(raw_payload, 'manufacturer', 'name') as manufacturer_name,
    JSONExtractString(raw_payload, 'manufacturer', 'inn') as manufacturer_inn,
    now() as load_datetime
FROM kafka_products_queue;


-- Таблица покупателей
CREATE TABLE raw_customers (
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    birth_date Date,
    gender String,
    registration_date DateTime,
    is_loyalty_member Int8,
    loyalty_card_number String,
    store_id String,
    country String,
    city String,
    street String,
    house String,       -- Используем String (на случай домов типа 15А)
    apartment String,   -- Используем String по той же причине
    postal_code String, -- Используем String на случай если индекс будет слишком большим и выйдет за пределы
    preferred_language String,
    preferred_payment_method String,
    receive_promotions Int8,
    load_datetime DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY customer_id;


CREATE TABLE kafka_customers_queue (raw_payload String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'customers',
kafka_group_name = 'ch_customers_group', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW mv_customers_etl TO raw_customers AS
SELECT
    JSONExtractString(raw_payload, 'customer_id') as customer_id,
    JSONExtractString(raw_payload, 'first_name') as first_name,
    JSONExtractString(raw_payload, 'last_name') as last_name,
    JSONExtractString(raw_payload, 'email') as email,
    JSONExtractString(raw_payload, 'phone') as phone,
    parseDateTimeBestEffort(JSONExtractString(raw_payload, 'birth_date')) as birth_date,
    JSONExtractString(raw_payload, 'gender') as gender,
    parseDateTimeBestEffort(JSONExtractString(raw_payload, 'registration_date')) as registration_date,
    JSONExtractBool(raw_payload, 'is_loyalty_member') as is_loyalty_member,
    JSONExtractString(raw_payload, 'loyalty_card_number') as loyalty_card_number,
    JSONExtractString(raw_payload, 'purchase_location', 'store_id') as store_id,
    JSONExtractString(raw_payload, 'delivery_address', 'country') as country,
    JSONExtractString(raw_payload, 'delivery_address', 'city') as city,
    JSONExtractString(raw_payload, 'delivery_address', 'street') as street,
    JSONExtractString(raw_payload, 'delivery_address', 'house') as house, -- String в случае если номер дома 43А
    JSONExtractString(raw_payload, 'delivery_address', 'apartment') as apartment, -- String по той же причине как и с номером дома
    JSONExtractString(raw_payload, 'delivery_address', 'postal_code') as postal_code, -- Индекс большое число, может выйти за пределы и упасть
    JSONExtractString(raw_payload, 'preferences', 'preferred_language') as preferred_language,
    JSONExtractString(raw_payload, 'preferences', 'preferred_payment_method') as preferred_payment_method,
    JSONExtractBool(raw_payload, 'preferences', 'receive_promotions') as receive_promotions,
    now() as load_datetime
FROM kafka_customers_queue;

-- Таблица магазинов
CREATE TABLE raw_stores (
    store_id String,
    store_name String,
    store_network String,
    store_description String,
    type String,
    categories Array(String),
    manager_name String,
    manager_phone String,
    manager_email String,
    country String,
    city String,
    street String,
    house String,
    postal_code String,
    location_coordinates Tuple(latitude Float64, longitude Float64), -- Именованный кортеж
    accept_online_orders Int8,
    delivery_available Int8,
    warehouse_connected Int8,
    last_inventory_date Date,
    load_datetime DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY store_id;

CREATE TABLE kafka_stores_queue (raw_payload String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'stores',
kafka_group_name = 'ch_stores_group', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW mv_stores_etl TO raw_stores AS
SELECT
    JSONExtractString(raw_payload, 'store_id') as store_id,
    JSONExtractString(raw_payload, 'store_name') as store_name,
    JSONExtractString(raw_payload, 'store_network') as store_network,
    JSONExtractString(raw_payload, 'store_type_description') as store_description,
    JSONExtractString(raw_payload, 'type') as type,
    JSONExtract(raw_payload, 'categories', 'Array(String)') as categories,
    JSONExtractString(raw_payload, 'manager', 'name') as manager_name,
    JSONExtractString(raw_payload, 'manager', 'phone') as manager_phone,
    JSONExtractString(raw_payload, 'manager', 'email') as manager_email,
    JSONExtractString(raw_payload, 'location', 'country') as country,
    JSONExtractString(raw_payload, 'location', 'city') as city,
    JSONExtractString(raw_payload, 'location', 'street') as street,
    JSONExtractString(raw_payload, 'location', 'house') as house,
    JSONExtractString(raw_payload, 'location', 'postal_code') as postal_code,
    JSONExtract(raw_payload, 'location', 'coordinates', 'Tuple(latitude Float64, longitude Float64)') as location_coordinates,
    JSONExtractBool(raw_payload, 'accepts_online_orders') as accept_online_orders,
    JSONExtractBool(raw_payload, 'delivery_available') as delivery_available,
    JSONExtractBool(raw_payload, 'warehouse_connected') as warehouse_connected,
    toDate(JSONExtractString(raw_payload, 'last_inventory_date')) as last_inventory_date,
    now() as load_datetime
FROM kafka_stores_queue;

-- Самая важная таблица Покупок. Будем хранить данные в плоском формате, идеально для аналитики
-- Один товар - одна строка
-- Для это используем ARRAY JOIN JSONExtractArrayRaw(raw_payload, 'items') AS item; в MV

CREATE TABLE raw_purchases (
    purchase_id String,
    purchase_datetime DateTime,
    customer_id String,
    store_id String,
    product_id String,
    product_name String,
    product_category String,
    quantity Float64,
    price_per_unit Float64,
    total_item_price Float64,
    total_amount Float64, -- общая сумма всего чека
    payment_method String,
    is_delivery Int8,
    load_datetime DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (purchase_datetime, store_id, product_id);


CREATE TABLE kafka_purchases_queue (raw_payload String)
ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'purchases',
    kafka_group_name = 'ch_purchases_group',
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW mv_purchases_etl TO raw_purchases AS
SELECT
    JSONExtractString(raw_payload, 'purchase_id') as purchase_id,
    parseDateTimeBestEffort(JSONExtractString(raw_payload, 'purchase_datetime')) as purchase_datetime,
    JSONExtractString(raw_payload, 'customer', 'customer_id') as customer_id,
    JSONExtractString(raw_payload, 'store', 'store_id') as store_id,

    -- Разворачиваем массив items
    JSONExtractString(item, 'product_id') as product_id,
    JSONExtractString(item, 'name') as product_name,
    JSONExtractString(item, 'category') as product_category,
    JSONExtractFloat(item, 'quantity') as quantity,
    JSONExtractFloat(item, 'price_per_unit') as price_per_unit,
    JSONExtractFloat(item, 'total_price') as total_item_price,

    JSONExtractFloat(raw_payload, 'total_amount') as total_amount,
    JSONExtractString(raw_payload, 'payment_method') as payment_method,
    JSONExtractBool(raw_payload, 'is_delivery') as is_delivery,
    now() as load_datetime
FROM kafka_purchases_queue
-- Магия: превращаем один JSON с N товарами в N строк таблицы
ARRAY JOIN JSONExtractArrayRaw(raw_payload, 'items') AS item;
-- Создаем новую БД Silver с читсыми данными, движок будет  ReplacingMergeTree чтобы избавиться от дубликатов
CREATE DATABASE IF NOT EXISTS silver;

-- 1. Создаем чистую таблицу (уже в базе silver)
CREATE TABLE silver.products (
    id String,
    name String,
    product_group String, -- переименовали для удобства
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
) ENGINE = ReplacingMergeTree()
ORDER BY id;

-- 2. Материализованное представление (ETL пайплайн)
CREATE MATERIALIZED VIEW default.mv_products_clean TO silver.products AS
SELECT
    lower(trim(id)) as id,
    lower(trim(name)) as name,
    lower(trim(`group`)) as product_group,
    lower(trim(description)) as description,
    kbju_calories,
    kbju_protein,
    kbju_fat,
    kbju_carbohydrates,
    price,
    lower(trim(unit)) as unit,
    lower(trim(origin_country)) as origin_country,
    expiry_days,
    is_organic,
    lower(trim(barcode)) as barcode,
    lower(trim(manufacturer_name)) as manufacturer_name,
    lower(trim(manufacturer_inn)) as manufacturer_inn,
    now() as load_datetime
FROM default.raw_products
WHERE
    -- Проверка на NULL и пустые строки
    id IS NOT NULL AND id != ''
    AND name IS NOT NULL AND name != ''
    -- Проверка адекватности
    AND price > 0
    AND expiry_days >= 0;

 CREATE TABLE silver.customers (
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
    house String,
    apartment String,
    postal_code String,
    preferred_language String,
    preferred_payment_method String,
    receive_promotions Int8,
    load_datetime DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY customer_id;

CREATE MATERIALIZED VIEW default.mv_customers_clean TO silver.customers AS
SELECT
    -- Приведение ID и строковых данных к нижнему регистру
    lower(trim(customer_id)) as customer_id,
    lower(trim(first_name)) as first_name,
    lower(trim(last_name)) as last_name,
    lower(trim(email)) as email,
    lower(trim(phone)) as phone,
    birth_date,
    lower(trim(gender)) as gender,
    registration_date,
    is_loyalty_member,
    lower(trim(loyalty_card_number)) as loyalty_card_number,
    lower(trim(store_id)) as store_id,
    lower(trim(country)) as country,
    lower(trim(city)) as city,
    lower(trim(street)) as street,
    lower(trim(house)) as house,
    lower(trim(apartment)) as apartment,
    lower(trim(postal_code)) as postal_code,
    lower(trim(preferred_language)) as preferred_language,
    lower(trim(preferred_payment_method)) as preferred_payment_method,
    receive_promotions,
    now() as load_datetime
FROM default.raw_customers
WHERE
    -- 1. Проверка на пустые ключевые поля
    customer_id != '' AND customer_id IS NOT NULL
    -- 2. Проверка адекватности дат (не из будущего)
    AND birth_date <= today()
    AND registration_date <= now();

 -- Таблица stores
 CREATE TABLE silver.stores (
    store_id String,
    store_name String,
    store_network String,
    store_description String,
    type String,
    categories Array(String), -- Массив остается массивом
    manager_name String,
    manager_phone String,
    manager_email String,
    country String,
    city String,
    street String,
    house String,
    postal_code String,
    location_coordinates Tuple(latitude Float64, longitude Float64),
    accept_online_orders Int8,
    delivery_available Int8,
    warehouse_connected Int8,
    last_inventory_date Date,
    load_datetime DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY store_id;

CREATE MATERIALIZED VIEW default.mv_stores_clean TO silver.stores AS
SELECT
    lower(trim(store_id)) as store_id,
    lower(trim(store_name)) as store_name,
    lower(trim(store_network)) as store_network,
    lower(trim(store_description)) as store_description,
    lower(trim(type)) as type,
    -- Мы "проходимся" по каждому элементу x и применяем lower(trim(x))
    arrayMap(x -> lower(trim(x)), categories) as categories,
    lower(trim(manager_name)) as manager_name,
    lower(trim(manager_phone)) as manager_phone,
    lower(trim(manager_email)) as manager_email,
    lower(trim(country)) as country,
    lower(trim(city)) as city,
    lower(trim(street)) as street,
    lower(trim(house)) as house,
    lower(trim(postal_code)) as postal_code,
    location_coordinates,
    accept_online_orders,
    delivery_available,
    warehouse_connected,
    last_inventory_date,
    now() as load_datetime
FROM default.raw_stores
WHERE
    store_id != '' AND store_id IS NOT NULL
    AND store_name != '' AND store_name IS NOT NULL
    -- Проверка: инвентаризация не может быть в будущем
    AND last_inventory_date <= today();


-- Последняя и самая важная таблица purchases
CREATE TABLE silver.purchases (
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
    total_amount Float64,
    payment_method String,
    is_delivery Int8,
    load_datetime DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(load_datetime) -- Вставляем load_datetime, чтобы в случае изменения заказа CH оставли новую версию
-- Ключ уникальности: один и тот же товар в одном и том же чеке
ORDER BY (purchase_id, product_id);

CREATE MATERIALIZED VIEW default.mv_purchases_clean TO silver.purchases AS
SELECT
    -- 1. Нижний регистр и очистка пробелов для ID
    lower(trim(purchase_id)) as purchase_id,
    purchase_datetime,
    lower(trim(customer_id)) as customer_id,
    lower(trim(store_id)) as store_id,
    lower(trim(product_id)) as product_id,
    -- 2. Текстовые поля в нижний регистр
    lower(trim(product_name)) as product_name,
    lower(trim(product_category)) as product_category,
    lower(trim(payment_method)) as payment_method,
    -- 3. Числовые значения (оставляем как есть, если они адекватны)
    quantity,
    price_per_unit,
    total_item_price,
    total_amount,
    is_delivery,
    now() as load_datetime
FROM default.raw_purchases
WHERE
    -- 4. Проверка на пустые строки в ключевых полях
    purchase_id != '' AND purchase_id IS NOT NULL
    AND product_id != '' AND product_id IS NOT NULL
    -- 5. Проверка адекватности дат (не из будущего)
    AND purchase_datetime <= now()
    -- 6. Проверка адекватности цен и количеств
    AND quantity > 0
    AND price_per_unit >= 0;


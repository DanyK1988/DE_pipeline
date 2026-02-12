from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ClickHouseToPySpark") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.9.6") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
    .getOrCreate()

purchase_schema = 'purchase_id STRING, purchase_datetime TIMESTAMP, customer_id STRING, store_id STRING, product_id STRING, product_name STRING, product_category STRING, quantity DOUBLE, price_per_unit DOUBLE, total_item_price DOUBLE, total_amount DOUBLE, payment_method STRING, is_delivery INTEGER, load_datetime TIMESTAMP'
# Параметры подключения
# Используем localhost, так как стучимся с твоего компьютера на внешний порт
ch_options_purcases = {
    "url": "jdbc:clickhouse://localhost:9123/silver",
    "user": "user",            # твой логин из docker-compose
    "password": "strongpassword", # твой пароль
    "dbtable": "purchases",     # таблица, которую хочешь забрать
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "customSchema": f"{purchase_schema}"
}

# Читаем данные из ClickHouse в DataFrame
df = spark.read.format("jdbc").options(**ch_options_purcases).load()

# Посмотрим, что прилетело
df.show(5)
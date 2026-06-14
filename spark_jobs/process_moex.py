from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, explode

# Настройки MinIO (S3)
minio_endpoint = "minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_raw = "bronze"
bucket_silver = "silver"

spark = SparkSession.builder \
    .appName("MOEX_ETL") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Устанавливаем уровень логирования
spark.sparkContext.setLogLevel("WARN")

# Путь к JSON-файлу (прямой путь, без wildcard)
json_path = f"s3a://{bucket_raw}/moex/2026/06/14/rts_index.json"

try:
    df_raw = spark.read.json(json_path)
    print("JSON файл успешно прочитан")
except Exception as e:
    print(f"Ошибка чтения JSON: {e}")
    spark.stop()
    exit(1)

# Выводим схему
df_raw.printSchema()

# Разворачиваем массив data
if "data" in df_raw.columns:
    df_exploded = df_raw.select(
        col("instrument"),
        col("assetcode"),
        explode("data").alias("record")
    )

    # Извлекаем поля из record
    df_final = df_exploded.select(
        col("instrument"),
        col("assetcode"),
        col("record.TRADEDATE").alias("trade_date"),
        col("record.CLOSE").alias("close"),
        col("record.OPEN").alias("open"),
        col("record.HIGH").alias("high"),
        col("record.LOW").alias("low"),
        col("record.VOLUME").alias("volume")
    ).filter(col("trade_date").isNotNull())

    df_final = df_final.withColumn("date", to_date(col("trade_date"), "yyyy-MM-dd"))

    # Сохраняем в Parquet
    output_path = f"s3a://{bucket_silver}/moex_prices"
    df_final.write \
        .mode("overwrite") \
        .partitionBy("instrument", year("date").alias("year"), month("date").alias("month")) \
        .parquet(output_path)

    print(f"Данные сохранены в {output_path}")
    print(f"Количество записей: {df_final.count()}")
else:
    print("В JSON нет поля 'data'")

spark.stop()
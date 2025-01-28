from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull

# SparkSession oluştur
spark = SparkSession.builder \
    .appName("EDA with Spark") \
    .master("yarn") \
    .getOrCreate()

# Giriş ve çıkış yolları
input_path = "gs://bucket-miy-a/Home_and_Kitchen.jsonl.gz"
output_path = "gs://bucket-miy-a/output_for_pyspark_eda"

# JSONL dosyasını oku
data_df = spark.read.json(input_path)

# Eksik değerleri analiz et
missing_values = data_df.select([
    count(when(isnull(col(c)) | (col(c) == ""), c)).alias(c)
    if c != "images" else count(when(isnull(col(c)), c)).alias(c)  # images sütunu için özel kontrol
    for c in data_df.columns
])

# Sayısal sütunların istatistiklerini hesapla
numeric_summary = data_df.select("rating", "helpful_vote").summary()

# Rating dağılımını hesapla
rating_distribution = data_df.groupBy("rating").count()

# En çok inceleme alan ürünler
top_products = data_df.groupBy("parent_asin").count().orderBy(col("count").desc())

# Sonuçları JSON olarak kaydet
missing_values.write.json(f"{output_path}/missing_values")
numeric_summary.write.json(f"{output_path}/numeric_summary")
rating_distribution.write.json(f"{output_path}/rating_distribution")
top_products.write.json(f"{output_path}/top_products")

# SparkSession kapat
spark.stop()
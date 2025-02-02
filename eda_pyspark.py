from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull

spark = SparkSession.builder \
    .appName("EDA with Spark") \
    .master("yarn") \
    .getOrCreate()

input_path = "gs://bucket-miy-a/Home_and_Kitchen.jsonl.gz"
output_path = "gs://bucket-miy-a/output_for_pyspark_eda"

data_df = spark.read.json(input_path)

missing_values = data_df.select([
    count(when(isnull(col(c)) | (col(c) == ""), c)).alias(c)
    if c != "images" else count(when(isnull(col(c)), c)).alias(c)
    for c in data_df.columns
])

numeric_summary = data_df.select("rating", "helpful_vote").summary()

rating_distribution = data_df.groupBy("rating").count()

top_products = data_df.groupBy("parent_asin").count().orderBy(col("count").desc())

missing_values.write.json(f"{output_path}/missing_values")
numeric_summary.write.json(f"{output_path}/numeric_summary")
rating_distribution.write.json(f"{output_path}/rating_distribution")
top_products.write.json(f"{output_path}/top_products")

spark.stop()
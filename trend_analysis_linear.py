from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_unixtime, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("LinearRegressionTrendAnalysis") \
    .master("yarn") \
    .getOrCreate()

input_path = "gs://bucket-miy-a/Home_and_Kitchen.jsonl.gz"
output_path = "gs://bucket-miy-a/output_linear_regression"

data_df = spark.read.json(input_path)

data_df = data_df.withColumn("year_month", date_format(from_unixtime(col("timestamp") / 1000), "yyyy-MM"))

grouped_df = data_df.groupBy("parent_asin", "year_month").agg(
    avg("rating").alias("average_rating")
)

vector_assembler = VectorAssembler(inputCols=["average_rating"], outputCol="features")
feature_df = vector_assembler.transform(grouped_df)

train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="average_rating")
lr_model = lr.fit(train_data)

predictions = lr_model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="average_rating", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE): {rmse}")

predictions.select("parent_asin", "year_month", "average_rating", "prediction").write.json(output_path)

spark.stop()

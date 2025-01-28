from pyspark import SparkConf, SparkContext
import json

conf = SparkConf().setAppName("SparkMapperReducerClean").setMaster("yarn")
sc = SparkContext(conf=conf)

input_path = "gs://bucket-miy-a/Home_and_Kitchen.jsonl.gz"
output_path = "gs://bucket-miy-a/output"

data_rdd = sc.textFile(input_path)

def mapper(line):
    try:
        record = json.loads(line.strip())

        if not record.get("asin") or not record.get("rating"):
            return []

        record.pop("images", None)

        rating = record.get("rating")
        if not isinstance(rating, (int, float)) or not (1 <= rating <= 5):
            return []

        helpful_vote = record.get("helpful_vote", 0)
        if not isinstance(helpful_vote, int):
            helpful_vote = 0

        return [(record["asin"], {
            "asin": record["asin"],
            "total_rating": rating,
            "rating_count": 1,
            "helpful_vote": helpful_vote
        })]
    except json.JSONDecodeError:
        return []

def reducer(record1, record2):
    combined_record = {
        "asin": record1["asin"],
        "total_rating": record1["total_rating"] + record2["total_rating"],
        "rating_count": record1["rating_count"] + record2["rating_count"],
        "helpful_vote": record1["helpful_vote"] + record2["helpful_vote"]
    }
    return combined_record

cleaned_and_aggregated_rdd = data_rdd.flatMap(mapper).reduceByKey(reducer)

final_rdd = cleaned_and_aggregated_rdd.map(lambda x: {
    "asin": x[0],
    "average_rating": x[1]["total_rating"] / x[1]["rating_count"],
    "helpful_vote": x[1]["helpful_vote"]
})

final_rdd.map(json.dumps).saveAsTextFile(output_path)

sc.stop()

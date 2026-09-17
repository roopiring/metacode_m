"""Q4: Count whitespace-delimited words on a Spark Standalone cluster."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/opt/spark/data/wordcount.txt"


def main() -> None:
    spark = SparkSession.builder.appName("Q4 WordCount 김병필").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        lines = spark.read.text(INPUT_PATH)
        words = (
            lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
            .filter(F.col("word") != "")
        )

        top_twenty = (
            words.groupBy("word")
            .count()
            .orderBy(F.desc("count"), F.asc("word"))
            .limit(20)
        )

        print("Q4 WORDCOUNT TOP 20 (whitespace-delimited, case preserved)", flush=True)
        top_twenty.show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Transform the Q9 Netflix CSV into type-by-genre Parquet aggregates."""

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Container-local input CSV path")
    parser.add_argument("--output", required=True, help="Container-local output directory")
    parser.add_argument("--min-year", type=int, default=2015)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("q9-netflix-type-genre").getOrCreate()

    source = spark.read.option("header", True).option("multiLine", True).csv(args.input)
    aggregate = (
        source
        .withColumn("release_year", F.col("release_year").cast("int"))
        .filter(F.col("release_year") >= F.lit(args.min_year))
        .withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre") != "")
        .groupBy("type", "genre")
        .agg(F.count("*").alias("count"))
        .orderBy("type", "genre")
    )

    aggregate_rows = aggregate.count()
    print(f"Q9 minimum release_year: {args.min_year}")
    print(f"Q9 aggregate row count: {aggregate_rows}")
    aggregate.show(200, truncate=False)

    (
        aggregate.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output)
    )
    print(f"Q9 Parquet output: {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()

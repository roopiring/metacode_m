"""Q8: List, download, and count the provided Netflix CSV from S3."""

import argparse
import csv
import os
from pathlib import Path

import boto3


DEFAULT_KEY = "bronze/netflix_titles.csv"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "data" / "netflix_titles.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket",
        default=os.getenv("Q8_BUCKET"),
        required=os.getenv("Q8_BUCKET") is None,
        help="S3 bucket name (or set Q8_BUCKET)",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", "ap-southeast-2"),
        help="AWS region (default: ap-southeast-2)",
    )
    parser.add_argument("--key", default=DEFAULT_KEY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def list_bronze_objects(s3_client, bucket: str) -> None:
    print(f"[1/3] Objects under s3://{bucket}/bronze/", flush=True)
    paginator = s3_client.get_paginator("list_objects_v2")
    object_count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix="bronze/"):
        for item in page.get("Contents", []):
            object_count += 1
            print(f"- key={item['Key']} size={item['Size']} bytes", flush=True)
    if object_count == 0:
        raise FileNotFoundError(f"No objects found under s3://{bucket}/bronze/")


def download_object(s3_client, bucket: str, key: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    print(f"[2/3] Downloading s3://{bucket}/{key} -> {output}", flush=True)
    s3_client.download_file(bucket, key, str(output))
    print(f"Downloaded size={output.stat().st_size} bytes", flush=True)


def count_csv_records(csv_path: Path) -> int:
    # csv.reader treats a quoted field containing an embedded newline as one record.
    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)  # Exclude the header.
        return sum(1 for _ in reader)


def main() -> None:
    args = parse_args()
    s3_client = boto3.client("s3", region_name=args.region)

    list_bronze_objects(s3_client, args.bucket)
    download_object(s3_client, args.bucket, args.key, args.output)
    record_count = count_csv_records(args.output)

    print(f"[3/3] CSV records excluding header: {record_count}", flush=True)


if __name__ == "__main__":
    main()

import csv
import boto3
import gzip
import json
import unicodedata
from bs4 import BeautifulSoup
import html
import pandas as pd
import os
from io import StringIO

# Config
region = "us-east-1"
prefix_raw = "raw_docs/"
output_csv_key = "final_csv/news_cleaned.csv"

BUCKET_NAME = os.environ.get("RAW_BUCKET")
if not BUCKET_NAME:
    raise ValueError("Environment variable RAW_BUCKET is not set")
CLEANED_BUCKET = os.environ.get("CLEANED_BUCKET")
if not CLEANED_BUCKET:
    raise ValueError("Environment variable CLEANED_BUCKET is not set")


s3 = boto3.client("s3", region_name=region)

def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text) 
    text = BeautifulSoup(text, "html.parser").get_text()
    text = unicodedata.normalize("NFKD", text)
    return " ".join(text.lower().split())

def clean_documents_to_csv():
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix_raw)

    records = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            raw_key = obj['Key']
            if not raw_key.endswith('.json.gz'):
                continue
            print(f"[PROCESS] {raw_key}")

            # Read and decompress
            raw_bytes = s3.get_object(Bucket=BUCKET_NAME, Key=raw_key)['Body'].read()
            data = json.loads(gzip.decompress(raw_bytes))

            # Clean fields
            cleaned_record = {
                "Date": data.get("Date", ""),
                "Time": data.get("Time", ""),
                "Tag": data.get("Tag", ""),
                "Author": clean_text(data.get("Author", "")),
                "Free": data.get("Free", ""),
                "Title": clean_text(data.get("Title", "")),
                "Content": clean_text(data.get("Content", "")),
                "URL": data.get("URL", "")
            }

            records.append(cleaned_record)

    if not records:
        print("No valid documents found.")
        return

    # Convert to DataFrame and CSV
    df = pd.DataFrame(records)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)

    # Upload to S3
    s3.put_object(Bucket=CLEANED_BUCKET, Key=output_csv_key, Body=csv_buffer.getvalue())
    print(f" Final CSV written to s3://{CLEANED_BUCKET}/{output_csv_key}")

if __name__ == "__main__":
    clean_documents_to_csv()

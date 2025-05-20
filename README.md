# Crypto News ETL Pipeline

## Overview

This project builds a batch ETL pipeline to scrape, clean, store, and visualize cryptocurrency news articles. The system uses:

- Python/Docker for scraping and cleaning
- S3 for intermediate storage
- AWS Glue to load into Snowflake
- Snowflake Views to clean up types and extract additional features
- Tableau to visualize trends from the ingested articles

## Folder Structure
```
crypto-news-etl/
├── airflow/
│ └── dags/
│ ├── crypto_etl_docker_dag.py
│ └── crypto_news_etl_no_dockers.py
├── cleaner/
│ ├── cleaner.py
│ ├── Dockerfile
│ └── requirements.txt
├── scraper/
│ ├── scraper.py
│ ├── Dockerfile
│ └── requirements.txt
├── data/
│ └── new_cleaned/ # cleaned data from S3 that is transferred to Snowflake
```


## 1. Setup Instructions

### AWS Credentials

Before running any script that touches S3, ensure your environment has valid AWS credentials.

Use either:
```
aws configure
```

or environment variables:
```
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```
### Environment Setup

- Use either Docker or run scripts locally, depending on your Airflow setup.

### Matching Names

- All script names used in DAGs must match actual files in scraper/ and cleaner/.

- If you rename files or folders, make sure to reflect that in:

  - Airflow DockerOperator commands

  - Docker build contexts

- Hardcode your S3 bucket name where required in scraper.py and cleaner.py

## 2. Pipeline Flow
### Step 1: Scraping News Articles
- The scraper.py script:

  - Scrapes article data from CryptoSlate

  - Saves raw compressed JSON files locally or directly into S3

  - Handles pagination and duplicate checking

- Docker is used to package the scraper and make it portable.

### Step 2: Cleaning Raw Data
- cleaner.py takes raw compressed JSON files, cleans and normalizes:

  - Fixes encoding

  - Drops malformed rows

  - Outputs CSV files with consistent string schema

- The cleaner script is also dockerized and configured to push cleaned CSVs into S3 (new_cleaned/).

### 3. AWS Glue: Load to Snowflake
- Use the Visual Glue Studio to build a job:

  - Input: S3 bucket containing cleaned CSVs (new_cleaned/)

  - Output: Snowflake table news_articles

- The table schema in Snowflake after loading looks like:
```
CREATE OR REPLACE TABLE news_articles (
  DATE     VARCHAR,
  TIME     VARCHAR,
  TAG      VARCHAR,
  AUTHOR   VARCHAR,
  FREE     VARCHAR,
  TITLE    VARCHAR,
  CONTENT  VARCHAR,
  URL      VARCHAR
);
```
- Database: crypto_news_db

- Schema: public

- Table: news_articles

### 4. Snowflake View Creation
To convert types and normalize fields, run the following in Snowflake:

```
CREATE OR REPLACE VIEW crypto_news_db.public.vw_news_articles AS
SELECT
  TO_DATE(date, 'Mon. DD, YYYY') AS date_parsed,
  time,
  LOWER(tag) AS tag,
  author,
  CASE 
    WHEN free ILIKE 'False' THEN TRUE 
    ELSE FALSE 
  END AS is_free,
  title,
  content,
  url,
  LENGTH(TRIM(content)) - LENGTH(REPLACE(TRIM(content), ' ', '')) + 1 AS word_count
FROM crypto_news_db.public.news_articles;
```
This view:

- Parses date into standard date format

- Converts tag to lowercase

- Converts free string to boolean

- Computes word_count from content

### 5. Visualization (Tableau)

![Dashboard Overview](https://github.com/user-attachments/assets/7f11fdbe-44bf-4f56-95ec-91acd18d8ad8)

CryptoSlate Analysis Dashboard
The final Tableau dashboard visualizes trends in crypto news articles stored in Snowflake. It includes:

- Key Stats:

  - Total Articles: 21,427

  - Average Word Count: 1,462

  - Top Tag: adoption

  - Top Author: oluwapelumi adejumo

Charts & Visuals:

  - Line chart of article counts over time (by parsed date)

  - Horizontal bar chart of top tags

  - Horizontal bar chart of top authors

  - Pie chart showing distribution of paid vs unpaid articles


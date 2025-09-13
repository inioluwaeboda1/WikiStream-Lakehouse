# WikiStream-Lakehouse
End-to-end AWS lakehouse for Wikipedia RecentChange: streams via SSE → Kafka, lands raw NDJSON to S3, curates to partitioned Parquet with Glue + Crawler, computes daily rollups in Athena, and upserts a serving mart in Postgres (RDS). Orchestrated by Airflow and fully reproducible with Docker Compose.

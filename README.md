Wiki Lakehouse (Kafka → S3/Glue → Athena)
=========================================

**What it is**An end-to-end, containerized pipeline that streams Wikipedia RecentChange events, lands raw data in S3, curates it to partitioned Parquet with AWS Glue (plus a Glue Crawler for schema), and materializes daily aggregates in Athena. Airflow orchestrates everything; Docker Compose makes it reproducible.

Architecture (why each piece)
-----------------------------

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   SSE → Kafka → S3 (raw NDJSON)                  → Glue Job + Crawler → S3 (curated Parquet, yyyy/mm/dd)                                       → Athena CTAS (daily_* tables)                                       → Explore via Athena/QuickSight   `

*   **Kafka**: decouples ingestion from downstream; can replay and scale consumers.
    
*   **S3 (raw)**: cheap, immutable landing; micro-batches by time/size.
    
*   **Glue Job**: normalizes JSON → Parquet; **partitioning** by yyyy/mm/dd for efficient scans.
    
*   **Glue Crawler**: updates the Data Catalog to keep Athena schema in sync.
    
*   **Athena CTAS**: idempotent daily materializations for fast, predictable queries.
    
*   **Airflow**: explicit lineage & retries; easy to extend with new tasks.
    

What’s in the repo
------------------
```text
├── dag/
│   ├── wiki_pipeline_dag.py        # Airflow DAG: clean ➜ sentiment
│   └── airflow_dag_screenshot.png      # Screenshot of Airflow DAG
├── docker-compose.yml                   # Minimal Airflow + Postgres (dev)
├── clean_transformed.py                 # Databricks: curate raw CSVs → Parquet
├── sentiment_analysis.py                # Databricks: LLM sentiment
├── README.md               
├── redShift_analysis.sql                # DDL + COPY + DQ checks + analysis portfolio
└── airbnb_exploratory_analysis.ipynb   
```


Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   airflow/    dags/wiki_pipeline_dag.py        # crawl_raw → curate_job → msck → daily_ctas    requirements.txt  glue/    job_wiki_curate.py               # PySpark transform + partitioning  kafka/    wiki-producer.py                 # SSE → Kafka    wiki-s3-consumer.py              # Kafka → S3 raw (NDJSON, gz)  athena/    (helpers / sample SQL)  docker-compose.yml                 # services + env wiring   `

Run it
------

1.  Create .env (same folder as docker-compose.yml):
    
```bash
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_REGION=us-east-1
S3_BUCKET=wiki-pipline-bucket
```  

2.  Bring up Airflow metadata and user:
    

```bash
docker compose up airflow-init
```  

3.  Start services:
    
```bash
docker compose up -d kafka airflow-scheduler airflow-webserver s3-consumer producer
```  

4.  Orchestrate:
    
```bash
docker compose exec airflow-webserver bash -lc 'airflow dags unpause wiki_pipeline'
docker compose exec airflow-webserver bash -lc 'airflow dags trigger wiki_pipeline'
# UI: http://localhost:8080  (admin / admin)
```  

Inspect data quickly (Athena)
-----------------------------


```sql
-- partitions repaired by the DAG (MSCK)
SELECT date(from_unixtime("timestamp")) AS dt, title, "user", count(*) AS edits
FROM curated
WHERE yyyy=2025 AND mm=9 AND dd=10
GROUP BY 1,2,3
ORDER BY edits DESC
LIMIT 20;

-- daily table created by CTAS task
SELECT * FROM "daily_top_titles_2025_09_10" LIMIT 20;
```

Design notes / thought process
------------------------------

*   **Cost & speed**: Parquet + partition pruning keeps Athena queries cheap and fast.
    
*   **Idempotency**: daily CTAS can re-run safely; raw S3 is immutable for audit/rebuilds.
    
*   **Ops**: backpressure handled at Kafka; retries & alerts via Airflow; schema tracked in Glue Catalog.
    
*   **Extensibility**: add more consumers (e.g., feature store, real-time metrics) without touching the producer.

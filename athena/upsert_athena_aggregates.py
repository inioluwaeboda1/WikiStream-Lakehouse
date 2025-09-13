# athena/upsert_athena_aggregates.py
"""
Athena -> RDS daily aggregate loader

- Runs a parameterized Athena query to aggregate edits per (date, user)
- Streams result pages from Athena
- Idempotently upserts into Postgres (RDS) with a composite PK (dt, username)

Notes:
- TARGET_DATE is YYYY-MM-DD (defaults to yesterday UTC)
- ATHENA_OUT must be an S3 path for query results (e.g., s3://.../athena-results/)
- Table wiki_daily_top_editors is created if missing; safe to re-run for the same day
"""

import os, time, sys
import boto3
import psycopg2
from datetime import date, timedelta

# ---- Config via environment ----
REGION = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DB", "wiki_db")
ATHENA_OUT = os.getenv("ATHENA_OUT")  # e.g. s3://bucket/athena-results/
TARGET_DATE = os.getenv("TARGET_DATE")  # YYYY-MM-DD; default: yesterday UTC
if not TARGET_DATE:
    TARGET_DATE = (date.today() - timedelta(days=1)).isoformat()

RDS_HOST = os.getenv("RDS_HOST")
RDS_DB   = os.getenv("RDS_DB", "wikidb")
RDS_USER = os.getenv("RDS_USER")
RDS_PW   = os.getenv("RDS_PASSWORD")
RDS_PORT = int(os.getenv("RDS_PORT", "5432"))

if not (ATHENA_OUT and RDS_HOST and RDS_USER and RDS_PW):
    print("Missing env: ATHENA_OUT, RDS_HOST, RDS_USER, RDS_PASSWORD", file=sys.stderr)
    sys.exit(2)

# ---- Athena SQL (one day slice, bots filtered) ----
SQL = f"""
WITH f AS (
  SELECT date(from_unixtime(timestamp)) AS dt,
         "user" AS username
  FROM curated
  WHERE coalesce(bot,false)=false
    AND date(from_unixtime(timestamp)) = date('{TARGET_DATE}')
)
SELECT dt, username, count(*) AS edits
FROM f
GROUP BY 1,2
"""

def athena_query(sql: str):
    """Execute SQL in Athena and return all rows (as lists of strings)."""
    ath = boto3.client("athena", region_name=REGION)
    qid = ath.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
    )["QueryExecutionId"]

    # Wait for completion (simple poll)
    while True:
        s = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if s in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if s != "SUCCEEDED":
        print(f"Athena query {s}", file=sys.stderr)
        sys.exit(1)

    # Paginate results and skip header row
    rows = []
    token = None
    while True:
        resp = ath.get_query_results(QueryExecutionId=qid, NextToken=token) if token else ath.get_query_results(QueryExecutionId=qid)
        rs = resp["ResultSet"]["Rows"]
        for i, r in enumerate(rs):
            if token is None and i == 0:  # header
                continue
            vals = [c.get("VarCharValue") for c in r["Data"]]
            rows.append(vals)
        token = resp.get("NextToken")
        if not token:
            break
    return rows

def upsert_pg(rows):
    """Create target table if needed and upsert daily records."""
    conn = psycopg2.connect(
        host=RDS_HOST, port=RDS_PORT, dbname=RDS_DB, user=RDS_USER, password=RDS_PW
    )
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        # Idempotent target (PK makes INSERT ... ON CONFLICT safe)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_daily_top_editors (
          dt date NOT NULL,
          username text NOT NULL,
          edits integer NOT NULL,
          PRIMARY KEY (dt, username)
        );
        """)
        for dt, username, edits in rows:
            cur.execute("""
                INSERT INTO wiki_daily_top_editors(dt, username, edits)
                VALUES (%s, %s, %s)
                ON CONFLICT (dt, username)
                DO UPDATE SET edits = EXCLUDED.edits;
            """, (dt, username, int(edits or 0)))
    conn.close()

if __name__ == "__main__":
    print(f"[Athena -> RDS] Loading {TARGET_DATE}")
    data = athena_query(SQL)
    print(f"Fetched {len(data)} rows from Athena")
    upsert_pg(data)
    print("Upsert complete.")

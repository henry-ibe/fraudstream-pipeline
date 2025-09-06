import os, io, json, time, gzip, hashlib, threading
from datetime import datetime

import boto3
import psycopg2
from psycopg2 import OperationalError
from fastapi import FastAPI

# ---------- Config ----------
REGION        = os.getenv("AWS_REGION", "us-east-1")
SQS_URL       = os.environ["SQS_URL"]
S3_BUCKET     = os.environ["S3_BUCKET"]
MODEL_VERSION = os.getenv("MODEL_VERSION", "baseline")

sqs = boto3.client("sqs", region_name=REGION)
s3  = boto3.client("s3",  region_name=REGION)

def pg_connect(retries: int = 10, delay: int = 3) -> psycopg2.extensions.connection:
    """Connect to Postgres with simple retries."""
    last = None
    for _ in range(retries):
        try:
            c = psycopg2.connect(
                host=os.environ["DB_HOST"],
                dbname=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASS"],
            )
            c.autocommit = True
            return c
        except Exception as e:
            last = e
            time.sleep(delay)
    raise last

conn = pg_connect()

# ---------- Helpers ----------
def score(tx: dict) -> float:
    """Tiny heuristic model for demo."""
    h = int(hashlib.sha256(tx["card_hash"].encode()).hexdigest(), 16) % 100
    base = tx["amount"] / 500.0
    return min(1.0, 0.3 * base + 0.7 * (h / 100.0))

def write_s3(tx: dict) -> None:
    """Append one JSON line (gzipped) into partitioned layout."""
    now = datetime.utcnow()
    key = f"tx/dt={now:%Y-%m-%d}/hr={now:%H}/part-{int(time.time())}.json.gz"
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="w") as gz:
        gz.write((json.dumps(tx) + "\n").encode())
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())

def write_db(tx: dict, s: float) -> None:
    """Dual-write to tx_raw and tx_scored with simple reconnect on failure."""
    global conn
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tx_raw(txn_id, amount, merchant_id, ts, payload)
                   VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (tx["txn_id"], tx["amount"], tx["merchant_id"], tx["ts"], json.dumps(tx)),
            )
            cur.execute(
                """INSERT INTO tx_scored(txn_id, ts, amount, merchant_id, score, features)
                   VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (
                    tx["txn_id"], tx["ts"], tx["amount"], tx["merchant_id"], s,
                    json.dumps({"model": MODEL_VERSION}),
                ),
            )
    except OperationalError:
        # Reconnect once if connection was dropped
        conn = pg_connect()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tx_raw(txn_id, amount, merchant_id, ts, payload)
                   VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (tx["txn_id"], tx["amount"], tx["merchant_id"], tx["ts"], json.dumps(tx)),
            )
            cur.execute(
                """INSERT INTO tx_scored(txn_id, ts, amount, merchant_id, score, features)
                   VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (
                    tx["txn_id"], tx["ts"], tx["amount"], tx["merchant_id"], s,
                    json.dumps({"model": MODEL_VERSION}),
                ),
            )

# ---- FastAPI + background consumer ----
app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True, "model": MODEL_VERSION}

def consumer_loop():
    idle_ticks = 0
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,  # long polling
            )
            msgs = (resp or {}).get("Messages", []) or []

            if not msgs:
                idle_ticks += 1
                # Heartbeat every ~200s if idle
                if idle_ticks % 10 == 0:
                    print("… idle, no messages yet", flush=True)
                continue

            idle_ticks = 0
            to_delete = []
            for m in msgs:
                try:
                    tx = json.loads(m["Body"])
                    s  = score(tx)
                    write_db(tx, s)
                    write_s3({**tx, "score": s, "model": MODEL_VERSION})
                    to_delete.append({"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]})
                except Exception as inner:
                    print(f"ERROR processing message {m.get('MessageId')}: {inner}", flush=True)

            if to_delete:
                sqs.delete_message_batch(QueueUrl=SQS_URL, Entries=to_delete)
                print(f"Processed {len(to_delete)} msgs", flush=True)

        except Exception as e:
            print(f"ERROR in consumer loop: {e}", flush=True)
            time.sleep(2)

@app.on_event("startup")
def start_consumer():
    try:
        print("🔎 Startup env:",
              {k: os.getenv(k, "<missing>") for k in
               ["AWS_REGION","SQS_URL","S3_BUCKET","DB_HOST","DB_NAME","DB_USER","MODEL_VERSION"]},
              flush=True)
        t = threading.Thread(target=consumer_loop, daemon=True)
        t.start()
        print("✅ Consumer loop started", flush=True)
    except Exception as e:
        print(f"💥 Failed to start consumer loop: {e}", flush=True)

import os, json, boto3, random, time, uuid, datetime as dt

sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
QUEUE_URL = os.getenv("SQS_URL")

while True:
    msgs = []
    for _ in range(10):
        tx = {
            "txn_id": str(uuid.uuid4()),
            "amount": round(random.uniform(800, 1200), 2),   # HIGH values to trigger fraud
            "merchant_id": f"m{random.randint(1,50)}",
            "card_hash": str(uuid.uuid4()),
            "ts": dt.datetime.utcnow().isoformat()
        }
        msgs.append({"Id": str(uuid.uuid4()), "MessageBody": json.dumps(tx)})
    sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=msgs)
    print("Sent", len(msgs))
    time.sleep(2)


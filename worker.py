import json
import os
import time
import boto3
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("worker")

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "cloud-fluent-jobs-status")

def main():
    sqs = boto3.client("sqs")
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_TABLE_NAME)

    logger.info("worker_started queue_url=%s", SQS_QUEUE_URL)

    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )

        messages = response.get("Messages", [])

        if not messages:
            logger.info("no_messages")
            time.sleep(POLL_SECONDS)
            continue

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            body = json.loads(msg["Body"])

            job_id = body["job_id"]
            payload = body["payload"]

            logger.info("job_received job_id=%s payload=%s", job_id, payload)

            now = datetime.now(timezone.utc).isoformat()

            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #s = :s, updated_at = :u",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": "RUNNING",
                    ":u": now
                }
            )

            try:
                if payload.get("should_fail"):
                    logger.error("job_failed_intentionally job_id=%s payload=%s", job_id, payload)
                    raise RuntimeError("intentional worker failure")

                delay = int(payload.get("delay", 1))
                time.sleep(delay)

                now = datetime.now(timezone.utc).isoformat()

                table.update_item(
                    Key={"job_id": job_id},
                    UpdateExpression="SET #s = :s, updated_at = :u",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":s": "SUCCEEDED",
                        ":u": now
                    }
                )

                logger.info("job_completed job_id=%s delay=%s", job_id, delay)

                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=receipt_handle,
                )

                logger.info("message_deleted job_id=%s", job_id)

            except Exception as e:
                now = datetime.now(timezone.utc).isoformat()

                table.update_item(
                    Key={"job_id": job_id},
                    UpdateExpression="SET #s = :s, updated_at = :u, error = :e",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":s": "FAILED",
                        ":u": now,
                        ":e": str(e)
                    }
                )

                logger.exception("job_failed job_id=%s", job_id)

                raise
            

if __name__ == "__main__":
    main()
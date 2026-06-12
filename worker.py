import json
import os
import time
import boto3
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("worker")

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))


def main():
    sqs = boto3.client("sqs")

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

            logger.info("job_received body=%s", body)

            delay = int(body.get("delay", 1))
            time.sleep(delay)

            logger.info("job_completed delay=%s", delay)

            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle,
            )

            logger.info("message_deleted")
            

if __name__ == "__main__":
    main()
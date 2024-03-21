import logging
import json
import azure.functions as func
from azure.storage.queue import QueueClient

URL = "https://sablepoc.queue.core.windows.net"
SAS_TOKEN= "HkivBHyKwXA3E8gW00UmYRfbet4iEJ6FaKn/0utP1pcWN4FU2MMM2bh5oUp4S8Y2Fccr+ZbsGB6++ASthuS8cg=="
QUEUE_NAME = "payloads"


queue = QueueClient(
    account_url=URL,
    credential=SAS_TOKEN,
    queue_name=QUEUE_NAME
)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        queue.send_message(req.get_json())
    except Exception as e:
        return func.HttpResponse(f"Failed to put to queue: {e}", status_code=500)

    return func.HttpResponse(f"Success: {req.get_json()}", status_code=200)
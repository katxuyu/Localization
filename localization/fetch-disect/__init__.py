# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import os, sys, ast
import json
from shutil import ExecError


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


def main(payload: str) -> str:    
    try:
        outs = []
        messages = queue.receive_messages(messages_per_page=32, visibility_timeout=120)
        for msg_batch in messages.by_page():
            for msg in msg_batch:
                
                req_body = ast.literal_eval(str(msg.content))
                

                if "uplink_message" in req_body and "decoded_payload" in req_body["uplink_message"] and "scan_data" in req_body["uplink_message"]["decoded_payload"]:
                    scan_data = req_body["uplink_message"]["decoded_payload"]["scan_data"]

                    for datum in scan_data:
                        if "mac" in datum and "rssi" in datum and "utc_time" in datum:
                            data = {
                                        "mac": datum["mac"],
                                        "rssi": datum["rssi"],
                                        "utc_time": datum["utc_time"] 
                                    }
                            outs.append(data)
                queue.delete_message(msg.id, msg.pop_receipt)
                #logging.info(f"data :{data}")
                #outs.append(disect_message(msg))

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f"Error in fetch-disect: {e}, {fname}, {exc_tb.tb_lineno}")
        return f"Error in fetch-disect: {e}, {fname}, {exc_tb.tb_lineno} :: {msg}"
    else:
        return outs
           

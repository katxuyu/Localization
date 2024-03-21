# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
import os, sys

URL = "https://sablepoc.queue.core.windows.net"
SAS_TOKEN= "HkivBHyKwXA3E8gW00UmYRfbet4iEJ6FaKn/0utP1pcWN4FU2MMM2bh5oUp4S8Y2Fccr+ZbsGB6++ASthuS8cg=="
QUEUE_NAME = "payload"

from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData

conn_str = "Endpoint=sb://pwxpayloads1.servicebus.windows.net/;SharedAccessKeyName=bridgetowebhooks;SharedAccessKey=N7u/gsKXYaylMTNheC8ysTlWuGGvDjbjv59qpJhds+Y="
client = EventHubProducerClient.from_connection_string(conn_str=conn_str, eventhub_name="pwxpayloads")
def send_payload_to_eh(to_send):
    
    event_data_batch = client.create_batch()
    for datum in to_send:
        event_data_batch.add(EventData(json.dumps(datum).encode('utf-8')))
    
    client.send_batch(event_data_batch)

def main(payloads: str) -> str:
    try:
        mac_data = {}
        for p in payloads:
            if p and 'Error' not in p[0]:
                for m in p:
                    if "mac" in m and "rssi" in m and "utc_time" and m:
                        if m['mac'] not in mac_data:
                            mac_data[m["mac"]] = {
                                                    "mac" : m['mac'],
                                                    "payloads" : [],
                                                    "utc_time": ''
                                                    }
                        # logging.info(f"POOOOP {mac_data}")
                        # logging.info(f"POOOOP {mac_data[m['mac']]['payloads']}")
                        # logging.info(f"POOOOP {m['rssi']}")
                        mac_data[m['mac']]['payloads'].append(m['rssi'])
                        mac_data[m['mac']]['utc_time'] = m['utc_time']

        for item in mac_data:             
            send_payload_to_eh(mac_data[item])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f"Error in aggregation: {e}, {fname}, {exc_tb.tb_lineno}")
        return f"Error in aggregation: {e}, {fname}, {exc_tb.tb_lineno}"
    
    return mac_data

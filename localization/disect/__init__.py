import logging
import json

import azure.functions as func

from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData

conn_str = "Endpoint=sb://ehble-poc.servicebus.windows.net/;SharedAccessKeyName=dev;SharedAccessKey=AMP1FcvpoAHIhhqdy+Ovu6bQwJiA6PHi8iITdHcapbE=;EntityPath=ble-poc"


def send_payload_to_eh(to_send):
    client = EventHubProducerClient.from_connection_string(conn_str=conn_str, eventhub_name="ble-poc")
    event_data_batch = client.create_batch()
    for datum in to_send:
        event_data_batch.add(EventData(json.dumps(datum).encode('utf-8')))
    
    client.send_batch(event_data_batch)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    """
    auth = req.headers.get("Authorization", None)
    if not auth:
        return func.HttpResponse(
            "Authentication error: Authorization header is missing",
            status_code=401
        )
    parts = auth.split()

    if parts[0].lower() != "bearer":
        return func.HttpResponse("Authentication error: Authorization header must start with ' Bearer'", status_code=401)
    elif len(parts) == 1:
        return func.HttpResponse("Authentication error: Token not found", status_code=401)
    elif len(parts) > 2:
        return func.HttpResponse("Authentication error: Authorization header must be 'Bearer <token>'", status_code=401)
    """

    try:
        req_body = req.get_json()
    except ValueError as e:
        return func.HttpResponse(f"Request error: {e}", status_code=501)

    if "uplink_message" not in req_body or "decoded_payload" not in req_body["uplink_message"] or "scan_data" not in req_body["uplink_message"]["decoded_payload"]:
        return func.HttpResponse(f"Request error: Required element not found in the payload", status_code=501)
    else:
        scan_data = req_body["uplink_message"]["decoded_payload"]["scan_data"]
    
    to_send = []
    for idx, datum in enumerate(scan_data):
        if "mac" not in datum or "rssi" not in datum or "utc_time" not in datum:
            return func.HttpResponse(f"Request error: Required element not found in the scan_data index {idx}", status_code=501)
        else:
            data = {
                        "mac": datum["mac"],
                        "rssi": datum["rssi"],
                        "utc_time": datum["utc_time"] 
                    }   
            to_send.append(data)
    try:
        send_payload_to_eh(to_send)
    except Exception as e:
        return func.HttpResponse(f"Internal server error: {e}", status_code=500)
    else:        
        return  func.HttpResponse(
                f"Success {to_send}",
                status_code=200
        )


# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging, os, sys
import json
from datetime import datetime, timedelta
from urllib import request
import requests

import azure.functions as func
import azure.durable_functions as df
from azure.storage.queue import QueueClient

URL = "https://sablepoc.queue.core.windows.net"
SAS_TOKEN= "HkivBHyKwXA3E8gW00UmYRfbet4iEJ6FaKn/0utP1pcWN4FU2MMM2bh5oUp4S8Y2Fccr+ZbsGB6++ASthuS8cg=="
QUEUE_NAME = "payloads"

queue = QueueClient(
    account_url=URL,
    credential=SAS_TOKEN,
    queue_name=QUEUE_NAME
)

def orchestrator_function(context: df.DurableOrchestrationContext):
    try:
        tasks = []
        properties = queue.get_queue_properties()
        count = properties.approximate_message_count

        if(count < 32):
            tasks.append(context.call_activity('fetch-disect'))
        else:
            #Collect n messages and convert it to tasks.
            for i in range((count//32) + 1):
                tasks.append(context.call_activity('fetch-disect'))

        #Run all tasks in parallel and wait for all the results.
        results = yield context.task_all(tasks)
        if results:
            results = list(filter(None, results))
            logging.info(f"WEEEEEEEEEEE {results}")
            
            #aggregiate results based on the format and send message to bin
            results = yield context.call_activity('aggregate-send', results)
            logging.info(f"WOOOOOOOOOOO {results}")

            #return "Finish"

            #sleep for one hour between cleanups
            # next_cleanup = context.current_utc_datetime + timedelta(seconds=5)
            # yield context.create_timer(next_cleanup)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f"Error in aggregation: {e}, {fname}, {exc_tb.tb_lineno}")
        req = requests.post("https://afble-poc.azurewebsites.net/orchestrators/orchestrate")
        return f"Error in aggregation: {e}, {fname}, {exc_tb.tb_lineno} :: Requested another orchestration. This one should close."
    else:
        return "Success!"
        #context.continue_as_new(None)
    

main = df.Orchestrator.create(orchestrator_function)
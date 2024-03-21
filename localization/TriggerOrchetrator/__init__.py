import datetime
import logging
import requests

import azure.functions as func

import azure.durable_functions as df

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
        #pass
    res = requests.post("https://afble-poc.azurewebsites.net/orchestrators/orchestrate")
    logging.info(f"Response: {res.status_code} ;; {res.text}")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

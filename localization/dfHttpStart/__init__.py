# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging, sys, os

import azure.functions as func
import azure.durable_functions as df



async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    try:
        client = df.DurableOrchestrationClient(starter)
        instance_id = await client.start_new(req.route_params["functionName"], None, None)

        #logging.info(f"Started orchestration with ID = '{instance_id}'.")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f"Error in aggregation: {e}, {fname}, {exc_tb.tb_lineno}")
        return f"Request error: {e}, {fname}, {exc_tb.tb_lineno}"
    else:
        return client.create_check_status_response(req, instance_id)
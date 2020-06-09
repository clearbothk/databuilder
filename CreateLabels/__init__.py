import logging

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    image_name = ""
    labels = []
    try:
        req_body = req.get_json()
        image_name = req_body["image_name"]
        labels = req_body["labels"]
    except ValueError:
        response = func.HttpResponse("Labels were not provided")
        response.status_code = 400
        return response

    labels = req_body["labels"]
    logging.info(labels)
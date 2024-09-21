import azure.functions as func
import logging
import json
import http.client
import os

app = func.FunctionApp()


@app.queue_trigger(arg_name="msg", queue_name="translation-requests", connection="AzureWebJobsStorage") 
@app.table_output(arg_name="outputTable",
                  table_name="translations",
                  connection="AzureWebJobsStorage")
def do_translation(msg: func.QueueMessage, outputTable: func.Out[str]) -> None:
    try:
        # Parse the queue message
        message_content = msg.get_body().decode('utf-8')
        message_json = json.loads(message_content)
        text = message_json.get('text', '')
        from_language = message_json.get('from', 'en')
        to_language = message_json.get('to', 'de')

        to_translate = {
            "from": from_language,
            "to": to_language,
            "text": text
        }

        conn = http.client.HTTPSConnection("google-translate113.p.rapidapi.com")

        payload = json.dumps(to_translate)

        headers = {
            'x-rapidapi-key': os.environ['RAPIDAPI_KEY'],
            'x-rapidapi-host': "google-translate113.p.rapidapi.com",
            'Content-Type': "application/json"
        }

        conn.request("POST", "/api/v1/translator/text", payload, headers)
        res = conn.getresponse()
        data = res.read()

        translation = json.loads(data.decode("utf-8"))['trans']


        # Prepare the entity to be inserted
        entity = {
            "PartitionKey": message_json.get('user', 'user'),
            "RowKey": message_json.get('id', str(msg.id)),
            "From": from_language,
            "To": to_language,
            "OriginalText": message_json.get('text', ''),
            "TranslatedText": translation
        }

        # Insert the entity into the table
        outputTable.set(json.dumps(entity))

        logging.info(f"Successfully wrote message to table storage. RowKey: {entity['RowKey']}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise
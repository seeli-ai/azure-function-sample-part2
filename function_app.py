import azure.functions as func
import logging
import json

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

        # Prepare the entity to be inserted
        entity = {
            "PartitionKey": message_json.get('user', 'user'),
            "RowKey": message_json.get('id', str(msg.id)),
            "OriginalMessage": message_json.get('text', ''),
            "TranslatedMessage": "Not translated yet"  # Placeholder for future translation
        }

        # Insert the entity into the table
        outputTable.set(json.dumps(entity))

        logging.info(f"Successfully wrote message to table storage. RowKey: {entity['RowKey']}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise
from datetime import datetime
from google.cloud import bigquery
from pydub import AudioSegment
from io import BytesIO
from google.cloud import storage
import os
from google.cloud import speech
from google.cloud.speech import enums
from google.cloud.speech import types

def transcribe_gcs(gcs_uri):
    """Asynchronously transcribes the audio file specified by the gcs_uri."""
    client = speech.SpeechClient()

    audio = types.RecognitionAudio(uri=gcs_uri)
    config = types.RecognitionConfig(
        encoding=enums.RecognitionConfig.AudioEncoding.FLAC,
        language_code='es-CO')

    operation = client.long_running_recognize(config, audio)

    print('Waiting for operation to complete...')
    response = operation.result(timeout=90)

    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
    return response

def makearray(ss):
    array = []
    for result in ss.results:
        text = result.alternatives[0].transcript
        array.append(text)
    return array
    
def bq_create_table():
    schema = [
    bigquery.SchemaField('source', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('text_content', 'STRING', mode='REPEATED'),
    bigquery.SchemaField('scrapping_timestamp', 'TIMESTAMP', mode='REQUIRED')
#    bigquery.SchemaField('seconds', 'FLOAT', mode='REQUIRED')
            ]
    
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('radio_scrapping')
# Prepares a reference to the table
    table_ref = dataset_ref.table('blue')
    try:
        bigquery_client.get_table(table_ref)
    except:
            table = bigquery.Table(table_ref, schema=schema)
            table = bigquery_client.create_table(table)
            print('table {} created.'.format(table.table_id))

def upload_to_bq(row):
            # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('radio_scrapping')
    table_ref = dataset_ref.table('blue')
    table = bigquery_client.get_table(table_ref)
    rows_to_insert = [
            row
    ]
    errors = bigquery_client.insert_rows(table, rows_to_insert)
    print(errors)
    assert errors == []

def get_seconds(name):
    name_s = "blue"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('blue-r')
    blob = bucket.get_blob(name)
    file = BytesIO(blob.download_as_string())
    a = AudioSegment.from_file(file)
    seconds = a.duration_seconds
    return seconds

def scrapper(data, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    name = "gs://blue-r/" + data['name']
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%Y-%m-%d %H:%M:%S")
    ss = transcribe_gcs(name)
    array = makearray(ss)
 #   seconds = get_seconds(name)
    bq_create_table()
    upload_to_bq(["Blue", array, timestampStr])

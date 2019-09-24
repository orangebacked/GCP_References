import requests 

import datetime

import google.cloud.storage

from lxml import etree

import re 

from lxml import html

import pandas as pd

from google.cloud import bigquery

from functools import reduce



#### filter function

def filtering(element):
    
    element = str(element)
    
    n = element.split("-")[-1]
    
    return bool(re.search("^\d\d\d\d\d.", n))

### scrapping each article 
def scrapping(article):

    r = requests.get(article)
    
    tree = html.fromstring(r.content)
    
    
    list_elementos_p = tree.xpath('.//p/text()')
    
    paragraph = reduce(lambda x,y: str(x) + y, list_elementos_p)
    paragraph = re.sub( r"©.+", "", paragraph)
    paragraph = re.sub( r"                            Agregue.+", "", paragraph)
    paragraph = re.sub( r"Agregue a sus temas de interés Registrándose.+", "", paragraph)
    paragraph = paragraph.replace("\n", "")
    
    title = str(tree.xpath('.//a[@href="#"]/text()')[0])
    
    script = tree.xpath('.//script[@type="application/ld+json"]')
    
    script = str(etree.tostring(script[0]))
    
    date = script.split('datePublished":')[1][1:20]
    
    hour = date.split("T")[1] 
    
    tag = r.url.split("/")[3]
    
    item_id = int(r.url.split("-")[-1])
    
    timestamp = str(datetime.datetime.utcnow())
    
    row = (str(title), str(date), str(hour), tag, str(paragraph), item_id, timestamp)

    return row




def loop_req():
    
    r=requests.get("https://www.larepublica.co")
    
    tree=html.fromstring(r.content)
    
    list_elements=tree.xpath("//a[contains(@href,'larepublica')]/@href")
    
    list_articles=list(set(filter(filtering, list_elements)))
            
    ## in order to create the df
    dflist = []
    for n,article in enumerate(list_articles):
        try:
            row = scrapping(article)
            upload_to_bq(row)
            row_l = [x for x in row]
            dflist.append(row_l)
            print(n)
        except:
            print(n,'fail')
        
            pass
        
        df = pd.DataFrame(dflist,columns=["title", "date", "hour", "tag", "text_content", "item_id", "timestamp"])
    return df




def bq_create_table():
    
    schema = [
    bigquery.SchemaField('title', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('date', 'DATETIME', mode='REQUIRED'),
    bigquery.SchemaField('hour', 'TIME', mode='REQUIRED'),
    bigquery.SchemaField('tag', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('text_content', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('item_id', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('scrapping_timestamp', 'STRING', mode='REQUIRED')
            ]
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
# Prepares a reference to the table
    table_ref = dataset_ref.table('la_republica')
    try:
        bigquery_client.get_table(table_ref)
    except:
            table = bigquery.Table(table_ref, schema=schema)
            table = bigquery_client.create_table(table)
            print('table {} created.'.format(table.table_id))
        
        
def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapp')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    h = now.hour
    blob = bucket.blob('la-republica/{}-{}-{}-{}.csv'.format(y, m, d, h))
    blob.upload_from_string(csv)
    
def upload_to_bq(row):
            # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
    table_ref = dataset_ref.table('la_republica')
    table = bigquery_client.get_table(table_ref)
    rows_to_insert = [
            row
    ]
    errors = bigquery_client.insert_rows(table, rows_to_insert)
    print(errors)
    assert errors == []
    
def scrapper(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        bq_create_table()
        df = loop_req()
        csv = df.to_csv()
        upload_bucket(csv)
    return csv

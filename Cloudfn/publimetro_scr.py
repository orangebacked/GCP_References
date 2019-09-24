import requests 

import datetime

import google.cloud.storage

from lxml import etree

import re 

from lxml import html

import pandas as pd

from google.cloud import bigquery

from functools import reduce

def filtering(x):
    a= lambda x: str(x).split(".")[-1].count("html") == 1
    b= lambda x: str(x).split("/")[-1].count("-")>=2
    return (a(x) and b(x))

def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapp')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    h = now.hour
    blob = bucket.blob('publimetro/{}-{}-{}-{}.csv'.format(y, m, d, h))
    blob.upload_from_string(csv)
    
def upload_to_bq(row):
            # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
    table_ref = dataset_ref.table('publimetro')
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
    bq_create_table()
    df = loop_req()
    csv = df.to_csv()
    upload_bucket(csv)
    return csv

def loop_req():
    
    r=requests.get("https://www.publimetro.co/co/")
    
    tree=html.fromstring(r.content)
    
    list_elements=tree.xpath("//a[contains(@href,'publimetro')]/@href")
    
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

def scrapping(article):
    
    
    r = requests.get('https://www.publimetro.co/co/{}'.format(article))
    
    tree = html.fromstring(r.content)
    
    content_text = tree.xpath('.//p[contains(@class="selectionShareable","")]/text()')
    
    ## iterate may be a lambda function
    content = reduce(lambda x, y: x + y, content_text)

    content=content.encode('latin-1', "ignore").decode('utf-8', "ignore")
    content = re.sub( r"Ahora puedes inscribirte para recibir en tu correo electrónico las principales noticias de Publimetro", "", content)
    content=re.sub( r"\n\n\n", "", content)
    content=re.sub(r"…Posted by  on","", content)
    content=re.sub( r"Lo Último de  en tu correo...+", "", content)
    content=re.sub(r"© Copyright 2009 - 2019 Metro International","", content)
    
    date = tree.xpath('.//script[@type="application/ld+json"]/text()')
    date=date[0].split('"datePublished": "')[1][:19]
    
    hour = date.split("T")[1]
    
    title=tree.xpath('.//h1/text()')[0].encode('latin-1').decode('utf-8')
    
    tags=r.url.split("/")[8]
    
    item_id = tree.xpath('.//script[@type="text/javascript"]/text()')[0]
    item_id=int(item_id.split("'article_id': '")[1][:7])

    timestamp = str(datetime.datetime.utcnow())
    
    row = (str(title), str(date), str(hour), tags, str(content), item_id, timestamp)
        
    return row

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
    table_ref = dataset_ref.table('publimetro')
    try:
        bigquery_client.get_table(table_ref)
    except:
            table = bigquery.Table(table_ref, schema=schema)
            table = bigquery_client.create_table(table)
            print('table {} created.'.format(table.table_id))


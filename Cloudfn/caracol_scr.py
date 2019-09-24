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

    a = lambda x: len(str(x).split("/") )>= 3   
    b = lambda x: str(x).split(":")[0].count("https") == 0
    c = lambda x: str(x).split(":")[0].count("http") == 0
    d= lambda x: str(x).split(".")[-1].count("pdf") == 0
    e= lambda x: str(x).split("/")[-1].count("-")>=4
    return (a(x) and b(x) and c(x) and d(x) and e(x))

def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapp')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    h = now.hour
    blob = bucket.blob('caracol/{}-{}-{}-{}.csv'.format(y, m, d, h))
    blob.upload_from_string(csv)
    
def upload_to_bq(row):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
    table_ref = dataset_ref.table('caracol')
    table = bigquery_client.get_table(table_ref)
    rows_to_insert = [
            row
    ]
    errors = bigquery_client.insert_rows(table, rows_to_insert)
    print(errors)
    assert errors == []

def loop_req():
    
    r=requests.get("https://noticias.caracoltv.com/")
    
    tree=html.fromstring(r.content)
    
    list_elements=tree.xpath("//a[contains(@href,'')]/@href")
    
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
    
    
    r = requests.get('https://noticias.caracoltv.com/{}'.format(article))
    
    tree = html.fromstring(r.content)
    
    content_text = tree.xpath('.//p/text()')
    
    ## iterate may be a lambda function
    content = reduce(lambda x, y: x + y, content_text)

    content = re.sub( r"Miembro de: Miembro de:.+", "", content)
    content=content.encode('latin-1').decode('utf-8')
    
    date = str(tree.xpath('.//meta[@property="article:published_time"]/@content')[0])
    
    date=reduce(lambda x,y: x+"-"+y, date.split("-")[:-1])
    
    hour = date.split("T")[1].split("-")[0]
    
    title=tree.xpath('.//title/text()')[0].encode('latin-1').decode('utf-8')
    
    tags=r.url.split("/")[4]
    
    item_id = int(tree.xpath('.//meta[@name="cXenseParse:articleid"]/@content')[0])

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
    table_ref = dataset_ref.table('caracol')
    try:
        bigquery_client.get_table(table_ref)
    except:
            table = bigquery.Table(table_ref, schema=schema)
            table = bigquery_client.create_table(table)
            print('table {} created.'.format(table.table_id))

def scrapper(request):
    """Responds to any HTTP request.
    Args:
        nothing
    Returns:
		csv file of the scrapped dataset
    """
    bq_create_table()
    df = loop_req()
    csv = df.to_csv()
    upload_bucket(csv)
    return csv

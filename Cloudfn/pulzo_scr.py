#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 13:32:31 2019

@author: orangebacked
"""

import requests 

import datetime

import google.cloud.storage

import re 

from lxml import html

import pandas as pd

from google.cloud import bigquery

from functools import reduce



#### filter function

def filtering(x):
    
    element=str(x)
    
    n=element.split("-")[-1]
    
    return bool(re.search("..\d\d\d\d.",n))

### scrapping each article 
def scrapping(article):

    r = requests.get("https://www.pulzo.com{}".format(article))
    
    tree = html.fromstring(r.content)
    
    content =  tree.xpath(".//p/text()")
     
    content = reduce((lambda x,y: x+y), content)
    
    content = content.replace("\n", "").replace("\t","")
    
    content = re.sub(r"Para que estés bien informado.+","",content)
    
    content = re.sub(r"o.+?Recibe en tu correo las noticias más importantes del día","",content)
    
    content = re.sub(r"Recibe en tu c","",content)
    
    title = str(tree.xpath("//h1[@class='article-title']/text()")[0])
     
    date =  str(tree.xpath(".//meta[@name='cXenseParse:recs:publishtime']/@content")[0])
    
    date = reduce(lambda x,y: x + "-" + y, date.split("-")[:-1])
    
    reduce(lambda x,y: x + "-" + y, date.split("-")[:-1])
    tag = tree.xpath("//meta[@property='article:tag']/@content")
    
    tags = {str(x) for x in tag}

    hour = date.split("T")[1].split("-")[0]
    
    item_id = str(tree.xpath("//meta[@property='ps:id']/@content")[0])
    
    timestamp = str(datetime.datetime.utcnow())
    
    row = (str(title), str(date), str(hour), tags, str(content), item_id, timestamp)
   
    return row


def loop_req():
    
    r = requests.get("https://www.pulzo.com/")
    
    tree = html.fromstring( r.content )
    
    list_elements = tree.xpath("//a/@href")
    
    list_articles = list(set(filter(filtering, list_elements)))
            
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
    bigquery.SchemaField('tag', 'STRING', mode='REPEATED'),
    bigquery.SchemaField('text_content', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('item_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('scrapping_timestamp', 'TIMESTAMP', mode='REQUIRED')
            ]
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
# Prepares a reference to the table
    table_ref = dataset_ref.table('pulzo')
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
    blob = bucket.blob('pulzo/{}-{}-{}-{}.csv'.format(y, m, d, h))
    blob.upload_from_string(csv)
    
def upload_to_bq(row):
            # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
    table_ref = dataset_ref.table('pulzo')
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

import requests 

from lxml import html

import pandas as pd

import datetime

from lxml import etree

import google.cloud.storage

import re

from google.cloud import bigquery

import html as hhhh

from functools import reduce


def scrapping(article):
    
    r1 = requests.get('https://www.semana.com{}'.format(article))

    tree = html.fromstring(r1.content)

    content = tree.xpath('.//div[@id="contentItem"]')
    
    text_content = etree.tostring(content[0], pretty_print=True)
    
    text_content = hhhh.unescape(str(text_content))

    text_content = re.sub(r"<.{0,200}>","",text_content)
    
    text_content = text_content.replace("\\n", "").replace("\r", "").replace("b'", "")

    date = tree.xpath('.//span[@class="date"]/text()')[0]
    
    date = date.replace("|", "")
    
    date = date.strip()
    
    list_date = date.split(" ")
    
    list_date[1]
    
    if list_date[2] == "PM":
        hour = str(int(list_date[1].split(":")[0]) + 12) + ":" + list_date[1].split(":")[1] + ":" + list_date[1].split(":")[2]
    else:
        hour = list_date[1]
    
    date = list_date[0].replace("/", "-").split("-")
    
    date = date[-1] + "-" + date[0] + "-" + date[1]

    tag = tree.xpath('.//a[@itemprop="articleSection"]/text()')[0]
    
    tag= hhhh.unescape(str(tag))
    
    title = tree.xpath('.//h1[@class="tittleArticuloOpinion"]/text()')[0]
    
    title = title.strip()
    
    title = hhhh.unescape(str(title))
    
    item_id = int(tree.xpath('.//input[@id="itemId"]/@value')[0])
    
    timestamp = str(datetime.datetime.utcnow())
    
    row = [str(title), str(date), str(hour), str(tag), str(text_content), item_id, timestamp]
    
    return row   


def loop_req():
    
    r = requests.get('https://www.semana.com')
    
    tree = html.fromstring(r.content)
    
    list_articles = tree.xpath('.//a[contains(@class,"article-h-link")]/@href')
    
    
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
        
        df=pd.DataFrame(dflist,columns=["title", "date", "hour", "tag", "text_content", "item_id", "timestamp"])
    return df



def upload_to_bq(row):
            # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('news_scrapping')
    table_ref = dataset_ref.table('semana')
    table = bigquery_client.get_table(table_ref)
    rows_to_insert = [row]
    errors = bigquery_client.insert_rows(table, rows_to_insert)
    print(errors)
    assert errors == []
    
def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapp')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    h = now.hour
    blob = bucket.blob('semana/{}-{}-{}-{}.csv'.format(y, m, d, h))
    blob.upload_from_string(csv)
    
    
	    
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
        df = loop_req()
        df.drop_duplicates(inplace = True)
        csv = df.to_csv()
        upload_bucket(csv)
      #  pandas_gbq.to_gbq(df, 'news_scrapping.semana', project_id="servisentimen-servipolitics", if_exists='append')
        return csv

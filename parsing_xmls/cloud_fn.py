from google.cloud import bigquery
import xmltodict
from google.cloud import storage

def bq_create_table():
    
    schema = [
    bigquery.SchemaField('Id_sup_type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Id_sup', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Id_name_sup', 'STRING', mode='NULLABLE'),   
    bigquery.SchemaField('net_total_amount_currency', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('net_total_amount', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('gross_total_amount_currency', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('gross_total_amount', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('hour', 'STRING', mode='REQUIRED'),       
    bigquery.SchemaField('city', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('city_subdiv', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('city_subent', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('district', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('address', 'STRING', mode='NULLABLE'),

    bigquery.SchemaField('Idcbc', 'STRING', mode='NULLABLE'), 
        
        
    bigquery.SchemaField('items', 'RECORD', mode="REPEATED",
        fields=[
            bigquery.SchemaField("Quantity", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("Item_des", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT", mode="NULLABLE")
        ],
    ),
        
        
            ]
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('data_xml')
# Prepares a reference to the table
    table_ref = dataset_ref.table('data_restaurantes')
    try:
        bigquery_client.get_table(table_ref)
    except:
            table = bigquery.Table(table_ref, schema=schema)
            table = bigquery_client.create_table(table)
            print('table {} created.'.format(table.table_id))
    
def upload_to_bq(row):
            # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('data_xml')
    table_ref = dataset_ref.table('data_restaurantes')
    table = bigquery_client.get_table(table_ref)
    rows_to_insert = [row]
    errors = bigquery_client.insert_rows(table, rows_to_insert)
    print(errors)
    assert errors == []
    
def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    client = storage.Client()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    return blob.download_as_string()

def transformit(a):
    doc = str(a)[2:-1]
    doc = doc.replace(":","")
    doc = xmltodict.parse(doc)
    a = dict(doc)
    Id_sup_type = a['Invoice']['cacAccountingSupplierParty']['cacParty']['cacPartyIdentification']['cbcID']['@schemeName']
    Id_sup = a['Invoice']['cacAccountingSupplierParty']['cacParty']['cacPartyIdentification']['cbcID']['#text']
    Id_sup = int(Id_sup)
    Id_name_sup = a['Invoice']['cacAccountingSupplierParty']['cacParty']['cacPartyName']['cbcName']
    ## record
    net_total_currency = a['Invoice']['cacLegalMonetaryTotal']['cbcLineExtensionAmount']['@currencyID']
    net_total_amount = a['Invoice']['cacLegalMonetaryTotal']['cbcLineExtensionAmount']['#text']
    ## record
    gross_total_currency = a['Invoice']['cacLegalMonetaryTotal']['cbcTaxInclusiveAmount']['@currencyID']
    gross_total_amount = a['Invoice']['cacLegalMonetaryTotal']['cbcLineExtensionAmount']['#text']      
    # Time
    date = a['Invoice']['cbcIssueDate']
    time = a['Invoice']['cbcIssueTime']
    time = time[:2] + ':' + time[2:4] + ":" + time[4:6]
    Idcbc = a['Invoice']['cbcID']
    city = a["Invoice"]["cacAccountingSupplierParty"]["cacParty"]["cacPartyLegalEntity"]["cacRegistrationAddress"]["cbcCityName"]
    city_subdiv = a["Invoice"]["cacAccountingSupplierParty"]["cacParty"]["cacPartyLegalEntity"]["cacRegistrationAddress"]["cbcCitySubdivisionName"]
    city_subent = a["Invoice"]["cacAccountingSupplierParty"]["cacParty"]["cacPartyLegalEntity"]["cacRegistrationAddress"]["cbcCountrySubentity"]
    district = a["Invoice"]["cacAccountingSupplierParty"]["cacParty"]["cacPartyLegalEntity"]["cacRegistrationAddress"]["cbcDistrict"]
    address = a["Invoice"]["cacAccountingSupplierParty"]["cacParty"]["cacPartyLegalEntity"]["cacRegistrationAddress"]["cacAddressLine"]["cbcLine"]
    list_items= []
    print(type(a['Invoice']['cacInvoiceLine']))
    try:
        for item in a['Invoice']['cacInvoiceLine']:
            Quantity =item['cbcInvoicedQuantity']['#text']
            Item_des = item['cacItem']['cbcDescription']
            price = item['cacPrice']['cbcPriceAmount']['#text']
            list_items.append({'Quantity':Quantity, 'Item_des': Item_des, 'price':price})
    except:
            Quantity =a['Invoice']['cacInvoiceLine']['cbcInvoicedQuantity']['#text']
            Item_des = a['Invoice']['cacInvoiceLine']['cacItem']['cbcDescription']
            price = a['Invoice']['cacInvoiceLine']['cacPrice']['cbcPriceAmount']['#text']
            
            list_items.append({'Quantity':Quantity, 'Item_des': Item_des, 'price':price})
        
    array = list_items
    bq_create_table()  
    print(Id_sup_type, Id_sup, Id_name_sup, net_total_currency, net_total_amount, gross_total_currency, gross_total_amount, date, time, Idcbc, city, city_subdiv, city_subent, district, address, array)
    upload_to_bq((Id_sup_type, Id_sup, Id_name_sup, net_total_currency, net_total_amount, gross_total_currency, gross_total_amount, date, time, Idcbc, city, city_subdiv, city_subent, district, address, array))
    
    return "success"



def triggerrs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    a = download_blob(file['bucket'], file['name'])
    transformit(a)

#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 15:06:46 2019

@author: orangebacked
"""

# coding: utf-8
# Python 2.7

### Libraries: logging library uses logging.info() to debug 

import logging
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import pandas as pd

### The Code Runs With:
### python2 try.py --project=proyecto-emiliano-isaza --runner=DataflowRunner --temp_location=gs://proyecto-emiliano-isaza/hhhh

def run(argv=None):
   import pandas as pd
   import numpy as np
   p = beam.Pipeline(options=PipelineOptions())

### first rule
   def change_with_rule0(x, upper, lower):
    
        if x > upper:
            return 1
        elif x < lower:
            return 1
        else:
            return 0
    
     
### second rule   
   def change_with_rule_bound(x, upper, lower):
        if x > upper:
            return round(upper)
        elif x < lower:
            return round(lower)
        else:
            return x

#### class to run a ParDo you can use a class by it hasn't worked for me

   class tests_and_replace(beam.DoFn):
        
       def process(self, element):
           
           data = list(element[1])
           df = pd.DataFrame(data)
           df= df.dropna(subset=['cantidad', 'precioventa'])
           
           try:
               
               std_d = np.std(np.array(df['cantidad']))*3
               median = np.median(df['cantidad'])
               upper = round(median + std_d)
               lower = round(median - std_d)
               df['cantidad_n'] = df['cantidad'].apply(change_with_rule_bound, args=(upper,lower))
               df['was_row_changed'] = df['cantidad'].apply(change_with_rule0, args=(upper,lower))
               df['cantidad_n'] = df['cantidad_n'].astype("str")

               df['precioventa']=df['precioventa'].mask(df==0).fillna(df.mean())
               std_d = np.std(np.array(df['precioventa']))*3
               median = np.median(df['precioventa'])
               upper = median + std_d
               lower = median - std_d
               df['precioventa_n'] = df['precioventa'].apply(change_with_rule_bound, args=(upper,lower))
               df['was_row_changed'] = df['precioventa'].apply(change_with_rule0, args=(upper,lower))
               df['precioventa_n'] = df['precioventa_n'].astype("str")
                    
               if (True in list(df['was_row_changed'] == 1)):
                   df['was_set_changed'] = 1
               else:
                   df['was_set_changed'] = 0
                   
               return(df.T.to_dict().values())
        
           except:
               pass
        


## TODO  query the data the input should be an arg            
   query = """ 
            SELECT
            *
            FROM (
            SELECT
              *,
              EXTRACT(WEEK
              FROM
                FechaTransaccion) AS Week
            FROM
              `proyecto-emiliano-isaza.pruebasnormalidad.New_Julio` )
              """
   some_query = (query)
   bq_source = beam.io.BigQuerySource(query=some_query, use_standard_sql=True)

## Code to read the data, the data read by dataflow is interpreted as a diccionary but after it is groupt by you must pass a list in order to iterate it 
   dictss  = (p
                  | "ReadFromBigQuery" >> beam.io.Read(bq_source)
                  | "code Week" >> beam.Map(lambda record: (((str(record["Week"]) + str(record["EAN"]) + str(record['Tipologia_comercial'])), record)))
                  | "groupby week" >> beam.GroupByKey()
#                   | "df" >> beam.ParDo(tests_and_replace()))
    
#### define the variables to write to a table in BQ

   project_id = "proyecto-emiliano-isaza"  # replace with your project ID
   dataset_id = 'pruebasnormalidad'  # replace with your dataset ID
   table_id = 'beam_all_rounded'  # replace with your table ID
   table_schema = ('CATEGORIA:STRING, CiudadM:STRING, Codigo:INTEGER, DESCRIPCION_FINAL:STRING, DPTO:STRING, EAN:INTEGER, FABRICANTE:STRING, Fechatransaccion:TIMESTAMP, MARCA:STRING, PRESENTACION:STRING, SECCION:STRING, SUBCATEGORIA:STRING, Sale_id:STRING, Tipologia_comercial:STRING, Week:INTEGER, cantidad:FLOAT, cantidad_n:FLOAT, precioventa:FLOAT, precioventa_n:FLOAT, totalventa:FLOAT, was_row_changed:FLOAT, was_set_changed:FLOAT')
### write to BQ ###
   dictss | 'Write' >> beam.io.WriteToBigQuery(
                    table=table_id,
                    dataset=dataset_id,
                    project=project_id,
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=int(100))  
                                
   result = p.run()
   result.wait_until_finish()

if __name__ == '__main__':
   logger = logging.getLogger().setLevel(logging.INFO)
   run()
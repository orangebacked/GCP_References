### Libraries: logging library uses logging.info() to debug 

import logging
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import pandas as pd

### The Code Runs With:
### python2 try.py --project=proyecto-emiliano-isaza --runner=DataflowRunner --temp_location=gs://proyecto-emiliano-isaza/hhhh
class ContactUploadOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
        '--I_TABLE', required=True, type=str,
        help='table to read In BQ.  This can be a local file or '
             'a file in a Google Storage Bucket.')
        
        parser.add_value_provider_argument(
            '--O_TABLE', required=True,type=str,
                        help='Output BQ table to write results to.')
def run(argv=None):
   import pandas as pd
   import numpy as np
   argss = PipelineOptions().view_as(ContactUploadOptions)
   p = beam.Pipeline(options=PipelineOptions())
### Cero Rule    
    
   def inconsistency(row):
        if row.cantidad*row.precioventa > row.totalventa:
            return row.totalventa/row.cantidad
        else:
            return row.precioventa
        
### first rule
   def change_with_rule0(x, upper, lower):
    
        if x > upper:
            return 1
        elif x < lower:
            return 1
        else:
            return 0
        
   def inconsistency_marker(row):
    
        if row.cantidad*row.precioventa > row.totalventa:
            return 1
        else:
            return 0
    
### second rule   
   def change_with_rule_bound(x, upper, lower):
        if x > upper:
            return upper
        elif x < lower:
            return lower
        else:
            return x

#### class to run a ParDo you can use a class by it hasn't worked for me

   class tests_and_replace(beam.DoFn):
        
       def process(self, element):
           
           data = list(element[1])
           df = pd.DataFrame(data)
           df= df.dropna(subset=['cantidad', 'precioventa'])
           try:
               df["precioventa_inconsistency"] = df.apply(inconsistency, axis=1)
               std_d = np.std(np.array(df["cantidad"]))*3
               median = np.median(df["cantidad"])
               upper = round(median + std_d)
               lower = round(median - std_d)
               df['cantidad_n'] = df["cantidad"].apply(change_with_rule_bound, args=(upper,lower))
               df['was_row_changed'] =df["cantidad"].apply(change_with_rule0, args=(upper,lower))
               df['was_cantidad_changed'] = df['cantidad'].apply(change_with_rule0, args=(upper,lower))
            
               df["precioventa_inconsistency"] = df.apply(inconsistency, axis=1)       
               df["precioventa_inconsistency"] = df["precioventa_inconsistency"].mask(df==0).fillna(df.mean())
               std_d = np.std(np.array(df["precioventa_inconsistency"]))*3
               median = np.median(df["precioventa_inconsistency"])
               upper = median + std_d
               lower = median - std_d
               df['precioventa_n'] = df["precioventa_inconsistency"].apply(change_with_rule_bound, args=(upper,lower))
               df['was_row_changed'] = df["precioventa_inconsistency"].apply(change_with_rule0, args=(upper,lower))
               df['was_price_changed'] = df["precioventa_inconsistency"].apply(change_with_rule0, args=(upper,lower))
               df["totalventa_n"] = df['precioventa_n']*df['cantidad_n']
               df["was_price_inconsistent"] = df.apply( inconsistency_marker, axis=1)
               df["totalventa_n"] = df["totalventa_n"].astype("str")
               df['precioventa_n'] = df["precioventa_n"].astype("str")
               df['precioventa_inconsistency'] = df["precioventa_inconsistency"].astype("str")
               df['cantidad_n'] = df['cantidad_n'].astype("str")
               df["was_price_inconsistent"] = df["was_price_inconsistent"].astype("str")
               
                
               if (True in list(df['was_row_changed'] == 1)):
                   df['was_set_changed'] = 1
               else:
                   df['was_set_changed'] = 0
                   
               return(df.T.to_dict().values())
        
           except:
               pass
        

   weeky=str(pd.to_datetime('now').week)

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
              `proyecto-emiliano-isaza.pruebasnormalidad.{}`)
            WHERE Week = {}
              """.format(argss.I_TABLE,weeky)
    
   some_query = (query)
   bq_source = beam.io.BigQuerySource(query=some_query, use_standard_sql=True)

## Code to read the data, the data read by dataflow is interpreted as a diccionary but after it is groupt by you must pass a list in order to iterate it 
   dictss  = (p
                  | "ReadFromBigQuery" >> beam.io.Read(bq_source)
                  | "code Week" >> beam.Map(lambda record: (((str(record["Week"]) + str(record["EAN"]) + str(record['Tipologia_comercial'])), record)))
                  | "groupby week" >> beam.GroupByKey()
                  | "df" >> beam.ParDo(tests_and_replace()))
    
#### define the variables to write to a table in BQ

   project_id = "proyecto-emiliano-isaza"  # replace with your project ID
   dataset_id = 'pruebasnormalidad'  # replace with your dataset ID
   table_id = str(argss.O_TABLE) # replace with your table ID
   table_schema = ('CATEGORIA:STRING, CiudadM:STRING, Codigo:INTEGER, DESCRIPCION_FINAL:STRING, DPTO:STRING, EAN:INTEGER, FABRICANTE:STRING, Fechatransaccion:TIMESTAMP, MARCA:STRING, PRESENTACION:STRING, SECCION:STRING, SUBCATEGORIA:STRING, Sale_id:STRING, Tipologia_comercial:STRING, Week:INTEGER, cantidad:FLOAT, cantidad_n:FLOAT, precioventa:FLOAT, precioventa_n:FLOAT, totalventa:FLOAT, was_row_changed:FLOAT, was_set_changed:FLOAT, was_cantidad_changed:FLOAT, was_price_changed:FLOAT, precioventa_inconsistency:FLOAT, totalventa_n:FLOAT, was_price_inconsistent:FLOAT')

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
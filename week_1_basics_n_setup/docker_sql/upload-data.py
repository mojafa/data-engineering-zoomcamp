# #!/usr/bin/env python
# # coding: utf-8

import argparse
import pandas as pd
# import pyarrow.parquet as pq
import os
from sqlalchemy import create_engine
from time import time
from prefect import flow, task

def parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file, engine = 'pyarrow')
    df.to_csv(csv_file, index=False)


def ingest(csv_file, table_name, engine, chunksize=100000):
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=chunksize)
    run = True
    while run:
        try:
            t_start = time()
            df = next(df_iter)
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f'inserted another chunk, took {t_end-t_start:.3f} seconds')
        except Exception:
            run = False


def main(params):
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5433'
    db = 'ny_taxi'
    table_name = 'green_taxi_trips'
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet'
    parquet_file = 'output.parquet'
    csv_file = 'output.csv'
    os.system(f'wget {url} -O {parquet_file}')
    parquet_to_csv(parquet_file, csv_file)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    ingest(csv_file, table_name, engine)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    args = parser.parse_args()
    main(args)
    # main_flow()


# def main(params):
#     user = params.user
#     password = params.password
#     host = params.host
#     port = params.port
#     db = params.db
#     table_name = params.table_name
#     url = params.url
#     filename= 'green_tripdata_2019-01.parquet'


#     os.system(f"wget {url} -O {filename}")

#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


#     # df.to_sql(name=table_name, con=engine, if_exists='append')



#     # df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)

#     # df = next(df_iter)

#     # df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
#     # df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)



#     # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

#     # df.to_sql(name="green_taxi_data", con=engine, if_exists='append')
#     while True:

#         try:

#             t_start = time()

#             parquet_table = pq.read_table(filename)
#             df = parquet_table.to_pandas()
#             df.to_csv('output.csv',index=False, sep='\t')

#             df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)

#             df = next(df_iter)

#             df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
#             df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

#             df.to_sql(name=table_name, con=engine, if_exists='append')

#             t_end = time()

#             print('inserted another chunk, took %.3f second' % (t_end - t_start))

#         except StopIteration:
#             print("Finished ingesting data into the postgres database")
#             break

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

#     parser.add_argument('--user', required=True, help='user name for postgres')
#     parser.add_argument('--password', required=True, help='password for postgres')
#     parser.add_argument('--host', required=True, help='host for postgres')
#     parser.add_argument('--port', required=True, help='port for postgres')
#     parser.add_argument('--db', required=True, help='database name for postgres')
#     parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
#     parser.add_argument('--url', required=True, help='url of the csv file')

#     args = parser.parse_args()

#     main(args)

# # #!/usr/bin/env python
# # # coding: utf-8

# # # In[34]:


# # import pandas as pd
# # # import fastparquet OR
# # import pyarrow.parquet as pq


# # # In[74]:


# # #!wget from nyc taxi data the data you want in parquet form
# # get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet')


# # # ## Read the Parquet file in Python

# # # In[75]:


# # parquet_file = 'green_tripdata_2019-01.parquet'
# # df = pd.read_parquet(parquet_file)


# # # ## Inspect the data
# # #

# # # In[76]:


# # df.head()


# # # In[77]:


# # parquet_table = pq.read_table(parquet_file)
# # df = parquet_table.to_pandas()
# # df.to_csv('green_tripdata_2019-01.csv')


# # # ###  Write parquet file to CSV without compression

# # # In[78]:


# # # df= pd.read_csv('green_tripdata_2019-01.csv', nrows=100)


# # # ### Write with gzip compression

# # # In[40]:


# # # csv_output_compressed = 'green_tripdata_2019-01.csv.gz'
# # # output = df.to_csv(csv_output_compressed, index=False, compression='gzip')


# # # In[79]:


# # df= pd.read_csv('green_tripdata_2019-01.csv', nrows=100)


# # # In[80]:


# # df.head()


# # # In[81]:


# # print(pd .io.sql.get_schema(df,name='green_taxi_data'))


# # # In[82]:


# # pd.to_datetime(df.lpep_pickup_datetime)


# # # In[83]:


# # df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
# # df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# # # In[84]:


# # print(pd .io.sql.get_schema(df,name='green_taxi_data'))


# # # In[85]:


# # from sqlalchemy import create_engine


# # # In[86]:


# # engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')


# # # In[87]:


# # engine.connect()


# # # In[88]:


# # print(pd .io.sql.get_schema(df,name='green_taxi_data', con=engine))


# # # ## Let's batch the csv file as its too large for loading to postgres

# # # In[89]:


# # df_iter= pd.read_csv('green_tripdata_2019-01.csv', iterator=True, chunksize=100000)


# # # In[90]:


# # df_iter


# # # In[91]:


# # df=next(df_iter)


# # # In[92]:


# # len(df)


# # # ### create table and insert data chunk as only the column labels
# # #

# # # In[94]:


# # # df.lpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# # # df.lpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# # # In[95]:


# # df.to_sql(name='green_taxi_data', con=engine, if_exists='replace')

# # # if a table with name exists 'replace'


# # # In[96]:


# # get_ipython().run_line_magic('time', "df.to_sql(name='green_taxi_data', con=engine, if_exists='append')")


# # # In[97]:


# # from time import time


# # # In[98]:


# # while True:
# #     t_start = time()

# #     df = next(df_iter)

# #     df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
# #     df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# #     df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

# #     t_end = time()

# #     print('inserted another chunk..., took %0.3f second' %(t_end-t_start))


# # # In[26]:


# # get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')


# # # In[99]:


# # df_zones = pd.read_csv('taxi+_zone_lookup.csv')


# # # In[109]:


# # df = pd.read_csv('taxi+_zone_lookup.csv')
# # df


# # # In[110]:


# # print(pd.io.sql.get_schema(df, name='green_taxi_zones'))


# # # In[111]:


# # df_zones.to_sql(name='zones', con=engine, if_exists='replace')


# # # In[ ]:

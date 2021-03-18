import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import time

# db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
# db_pass = urllib.parse.quote_plus("Devuser1@3")
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )

connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required
filename = f'C:/project/Server_data_download/server_original/CNDB_ACCOUNT_Records_20210308_1.csv'
# filename_1 = 'C:/project/Server_data_download/server_small_df/CNDB_5_columns.csv'
filename_2 = f'C:/project/Server_data_download/server_small_df/CNDB_final.csv'

start_time_copy_expert = time.time()
df_original = pd.read_csv(filename)
cols = ['UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source','SystemType']

data = []
for i in range(df_original.shape[0]):
    data.append(['2021-03-12','UpdatedBy', "{'1TF':'Inprocess'}", 'Source','SystemType'])

df_data = pd.DataFrame(data, columns = cols)

df_final = pd.concat([df_data,df_original],axis=1)
cols_final = ['UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source', 'AccountNumber', 'AccountTypeName', 'AmSwStatus',
        'ClientDirNumber', 'SystemType', 'CountryCode', 'GeographyName',
        'RegionName', 'IndustryName', 'SectorName', 'DpeContactEmail', 'DpeContactName']
df_final = df_final.reindex(columns=cols_final)
df_final.to_csv(filename_2,index=False)
table_name = 'tbl_ibm_cndb_staging'

sql = f"COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(filename_2))

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()
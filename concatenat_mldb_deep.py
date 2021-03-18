import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import time

### CT TEAM STAGIING DB 
# db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
# db_pass = urllib.parse.quote_plus("Devuser1@3")
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )

### DEEPAK LAPTOP DB 
db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )

connection = engine.raw_connection() #TODO connection pooling required
cursor = connection.cursor() #TODO connection pooling required

#HW_Baseline_MLDB_20220303.xlsx #TODO CHange to xlsx and code again with 
# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/HW_Baseline_MLDB_20210218.csv'
# csv_file = f'C:/project/Server_data_download/server_original/HW_Baseline_MLDB_20210218_1.csv'
csv_file = f'C:/project/feb24 _sample_file/hw_baseline.csv'

#pd.read_excel('CNDB_ACCOUNT_Records_20210303.xlsx')#, sheetname=0).to_csv('CNDB_ACCOUNT_Records_20210303.csv', index=False)
#xlrd.biffh.XLRDError: Excel xlsx file; not supported
# https://stackoverflow.com/questions/65254535/xlrd-biffh-xlrderror-excel-xlsx-file-not-supported

# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/MLDB_final.csv'
csv_load_staging = f'C:/project/Server_data_download/server_small_df/MLDB_final.csv'

start_time_copy_expert = time.time()
df_original = pd.read_csv(csv_file)
df_original.drop(df_original.columns[0], axis=1,inplace=True) # DROP COL == SR NO 
print("----df_original.shape------",df_original.shape)
#print(df_original.head(2))

ls_cols_staging = ['UpdatedOn', 'UpdatedBy', 'Source', 'MldbTags']

ls_append_rows = []
for i in range(df_original.shape[0]):
    ls_append_rows.append(['2021-03-12','Atom_Bridge', 'Source', "{'1TF':'Inprocess'}"])

df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
print("-----df_staging_append.shape--------",df_staging_append.shape)
#print(df_staging_append.head(2))
#
df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
cols_final = ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', \
				'HardwareState', 'AccountNumber', 'CustomerName', \
				'CampusId', 'State','Address1', 'Address2',
				'Address3','City', 'ZipCode','InternetFacingHw', \
				'HostName', 'ServerType', 'MldbTags']
#TODO = 'ClientDirNumber', to be REMOVED ? 
#TODO - SAtish Code - 'SystemType' mapped in middle 
#TODO -- cols_final - needs to come from CONFIG.ini

df_load_staging = df_load_staging.reindex(columns=cols_final) #TODO Satish Code - Why Reindex ? 
# df_load_staging.columns=cols_final
df_load_staging.to_csv(csv_load_staging,index=False)
print("----df_load_staging.shape----------",df_load_staging.shape)
print(df_load_staging.head(2))

table_name = 'tbl_ibm_mldb_staging'

sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(csv_load_staging))

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()

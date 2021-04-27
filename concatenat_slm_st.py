import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import time
from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
#CNDB_ACCOUNT_Records_20210303.csv #TODO CHange to xlsx and code again with
filepath = '/home/vcenteruser/ibm_new/sprint3/a_csv/27_03_data/SLM_ST_FinalSheet_MasterData.csv'
# filepath = f'C:/project/feb24 _sample_file/SLM_ST_FinalSheet_MasterData.csv'
if filepath[-3:].lower() == "csv":
     df_slm_st = pd.read_csv(filepath)
else:
     df_slm_st = pd.read_excel(filepath) #sheetnam

#pd.read_excel('CNDB_ACCOUNT_Records_20210303.xlsx')#, sheetname=0).to_csv('CNDB_ACCOUNT_Records_20210303.csv', index=False)
#xlrd.biffh.XLRDError: Excel xlsx file; not supported
# https://stackoverflow.com/questions/65254535/xlrd-biffh-xlrderror-excel-xlsx-file-not-supported
from datetime import datetime
csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/a_csv/27_03_data/SLM_ST_final_'+str(datetime.now().date())+'_30.csv'
# csv_load_staging = f'C:/project/Server_data_download/server_small_df/slmst_final_DEEP.csv'

start_time_copy_expert = time.time()
# df_original.drop(df_original.columns[0], axis=1,inplace=True) # DROP COL == SR NO
print("----df_original.shape------",df_slm_st.shape)
#print(df_original.head(2))



# cols_original = ['SR_NO','ACCOUNT_NUMBER','CLIENT_DIR_NAME','CLIENT_DIR_NUMBER','ACCOUNT_TYPE_NAME','ACCOUNT_STATUS',
#                'COUNTRY_CODE','GEOGRAPHY_NAME','REGION_NAME','INDUSTRY_NAME',
#                'SECTOR_NAME','DPE_CONTACT_EMAIL','DPE_CONTACT_NAME'
#                ]

cols_to_drop = []
# TODO -- 'ClientDirName'== DROP == Dropped till we get in new STAGING Structure 
df_slm_st.drop(columns=cols_to_drop, axis=1,inplace=True)

dict_cols_mapping = {
               'GoldenValue' : 'CndbIdCount', # CNDB_ID
               'Parent' : 'ServiceTypeMapping',
                    }
df_slm_st.rename(columns=dict_cols_mapping, inplace=True)
#print(df_original.columns)
ls_append_rows = []
ls_cols_staging = ['UpdatedBy','UpdatedOn',]
for i in range(df_slm_st.shape[0]):
    ls_append_rows.append(['IBM_SLM_ST',str(datetime.now().date())])

df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
print("-----df_staging_append.shape--------",df_staging_append.shape)
#print(df_staging_append.head(2))
df_load_staging = pd.concat([df_staging_append,df_slm_st],axis=1, join="inner") 
#TODO - Join will only work if we have - dict_cols_mapping
# cols_final = ['UpdatedOn', 'UpdatedBy', 'CNDB_ID', 'ServiceTypeMapping']
cols_final = ['UpdatedBy', 'UpdatedOn', 'CndbIdCount', 'ServiceTypeMapping']


#TODO -- cols_final - needs to come from CONFIG.ini

df_load_staging = df_load_staging.reindex(columns=cols_final) #TODO Satish Code - Why Reindex ?
import numpy as np

table_name = 'tbl_bridge_ibm_service_type_reference'

df_load_staging.to_csv(csv_load_staging,index=False)

print("----df_load_staging.shape----------",df_load_staging.shape)
print(df_load_staging.head(2))


sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(csv_load_staging))

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()


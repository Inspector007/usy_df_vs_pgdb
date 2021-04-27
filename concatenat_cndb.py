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
# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/CNDB_ACCOUNT_Records_20210303.csv'
csv_file = f'C:/project/Server_data_download/server_original/CNDB_ACCOUNT_Records_20210303.csv'
# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/CNDB_ACCOUNT_Records_20210303.csv'
#pd.read_excel('CNDB_ACCOUNT_Records_20210303.xlsx')#, sheetname=0).to_csv('CNDB_ACCOUNT_Records_20210303.csv', index=False)
#xlrd.biffh.XLRDError: Excel xlsx file; not supported
# https://stackoverflow.com/questions/65254535/xlrd-biffh-xlrderror-excel-xlsx-file-not-supported
from datetime import datetime
# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/CNDB_final_'+str(datetime.now().date())+'_17.csv'
csv_load_staging = f'C:/project/Server_data_download/server_small_df/cndb_final_DEEP.csv'
# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/CNDB_final_'+str(datetime.now().date())+'_.csv'


start_time_copy_expert = time.time()
df_original = pd.read_csv(csv_file)
# df_original.drop(df_original.columns[0], axis=1,inplace=True) # DROP COL == SR NO
print("----df_original.shape------",df_original.shape)
#print(df_original.head(2))



# cols_original = ['SR_NO','ACCOUNT_NUMBER','CLIENT_DIR_NAME','CLIENT_DIR_NUMBER','ACCOUNT_TYPE_NAME','ACCOUNT_STATUS',
#                'COUNTRY_CODE','GEOGRAPHY_NAME','REGION_NAME','INDUSTRY_NAME',
#                'SECTOR_NAME','DPE_CONTACT_EMAIL','DPE_CONTACT_NAME'
#                ]

cols_to_drop = ['SR_NO',]
# TODO -- 'ClientDirName'== DROP == Dropped till we get in new STAGING Structure 
df_original.drop(columns=cols_to_drop, axis=1,inplace=True)

dict_cols_mapping = {
               'ACCOUNT_NUMBER' : 'AccountNumber',
               'CLIENT_DIR_NUMBER' : 'ClientDirNumber',
               'ACCOUNT_TYPE_NAME' : 'AccountTypeName',
               'ACCOUNT_STATUS' : 'AmSwStatus',
               'COUNTRY_CODE' : 'CountryCode',
               'GEOGRAPHY_NAME' : 'GeographyName',
               'REGION_NAME' : 'RegionName',
               'INDUSTRY_NAME' : 'IndustryName',
               'SECTOR_NAME' : 'SectorName',
               'DPE_CONTACT_EMAIL' : 'DpeContactEmail',
               'DPE_CONTACT_NAME' : 'DpeContactName'
                    }
df_original.rename(columns=dict_cols_mapping, inplace=True)
#print(df_original.columns)
ls_append_rows = []
ls_cols_staging = ['UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source','AccountName','ServiceType','SystemType','ClientDirName']
for i in range(df_original.shape[0]):
    ls_append_rows.append([str(datetime.now().date()),'Atom_Bridge','{"1TF":"Inprocess"}', 'Source_CNDB','dummy_str_AccountName','dummy_str_SystemType','dummy_str_SystemType','dummy_ClientDirName'])

df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
print("-----df_staging_append.shape--------",df_staging_append.shape)
#print(df_staging_append.head(2))
df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner") 
#TODO - Join will only work if we have - dict_cols_mapping
cols_final = ['UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source','AccountName','ServiceType',
'AccountNumber','AccountTypeName','AmSwStatus','ClientDirNumber', 'SystemType','ClientDirName',
'CountryCode', 'GeographyName','RegionName', 'IndustryName', 'SectorName', 'DpeContactEmail', 'DpeContactName']
#TODO -- cols_final - needs to come from CONFIG.ini

# df_load_staging.columns=cols_final
df_load_staging = df_load_staging.reindex(columns=cols_final) #TODO Satish Code - Why Reindex ?
import numpy as np
#df_load_staging['AccountName'] = df_load_staging['AccountName'].mask(np.random.random(df_load_staging.shape) < .1)

# nanidx = df_load_staging['AccountName'].sample(frac=0.4).index
# df_load_staging['AccountName'][nanidx] = "" # EMPTY STRING #np.NaN # Wrong its inserting NULL 
#
table_name = 'tbl_bridge_ibm_cndb_staging'
df_cndb_inprocess = pd.read_sql_query('select distinct("AccountNumber") from '+table_name+' WHERE \"CndbTags\"->>\'1TF\'=\'Inprocess\'', con=engine)


def get_baseline_flag(cntbtags,flag):
     import json
     try:
          dummy_tags = json.loads(cntbtags)
          if flag:
               dummy_tags['baselineflag'] = 1
          else:
               dummy_tags['baselineflag'] = 0
          return json.dumps(dummy_tags)
     except e: 
          print("{0}".format(e))
     

if df_cndb_inprocess.empty:
     df_load_staging["CndbTags"] = df_load_staging['CndbTags'].apply(lambda x: get_baseline_flag(x,True))
     df_load_staging.to_csv(csv_load_staging,index=False)
else:
     df_load_staging_old = df_load_staging[df_load_staging.AccountNumber.isin(pd.to_numeric(df_cndb_inprocess.AccountNumber))]
     df_load_staging_old['CndbTags'] = df_load_staging_old['CndbTags'].apply(lambda x: get_baseline_flag(x,False))
     df_load_staging = df_load_staging[~df_load_staging.AccountNumber.isin(pd.to_numeric(df_cndb_inprocess.AccountNumber))]
     df_load_staging['CndbTags'] = df_load_staging['CndbTags'].apply(lambda x: get_baseline_flag(x,True))

     df_load_staging = pd.concat([df_load_staging_old,df_load_staging])
     df_load_staging.to_csv(csv_load_staging,index=False)

######
print("----df_load_staging.shape----------",df_load_staging.shape)
print(df_load_staging.head(2))

# table_name = 'tbl_ibm_cndb_staging'


sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(csv_load_staging))

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()


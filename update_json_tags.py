import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd
import json
from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
table_name = 'tbl_ibm_cndb_staging'
"""
['UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source', 'AccountNumber', 'AccountTypeName', 'AmSwStatus', 
'ClientDirNumber', 'SystemType', 'CountryCode', 'GeographyName', 
'RegionName', 'IndustryName', 'SectorName', 'DpeContactEmail', 'DpeContactName']
"""
df_cndb_statging = pd.read_sql_query('SELECT * FROM '+table_name+' where "UpdatedOn" IS NULL', con=engine)

UpdatedOn_list = []
UpdatedBy_list = []
CndbTags_list = []
Source_list = []
SystemType_list = []

rowcount = 25
for i in range(rowcount):
    UpdatedOn_list.append('2020-03-02')
    UpdatedBy_list.append('CNDB_staging_processing')
    CndbTags_list.append('{"key": {"key": "processed"}}')
    Source_list.append('CNDB_DATA')
    SystemType_list.append('SystemType')

df_cndb_statging['UpdatedOn'] = UpdatedOn_list
df_cndb_statging['UpdatedBy'] = UpdatedBy_list
df_cndb_statging['CndbTags'] = CndbTags_list
df_cndb_statging['Source'] = Source_list
df_cndb_statging['SystemType'] = SystemType_list

start_time_copy_expert = time.time()

df_cndb_statging.to_sql(table_name, con=engine, if_exists='replace', index=False)

conn.commit()
cursor.close()
conn.close()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()
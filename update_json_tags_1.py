import urllib.parse
from sqlalchemy import create_engine
import time

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
# df_cndb_statging = pd.read_sql_table(table_name, con=engine)
# df_cndb_statging = pd.read_sql_query('SELECT * FROM '+table_name+' where "UpdatedOn" IS NULL', con=engine)

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


# print(''.join(newupdate_list))

start_time_copy_expert = time.time()
sql_query= ""
for i in range(25):
    sql_query += 'UPDATE tbl_ibm_cndb_staging set "UpdatedOn" = now(), ' \
            '"UpdatedBy" = \'{0}\', ' \
            '"CndbTags" = \'{1}\', ' \
            '"Source" = \'{2}\',' \
            '"SystemType" = \'{3}\'' \
            'where "UpdatedOn" IS NULL;'.format(UpdatedBy_list[i],CndbTags_list[i],Source_list[i],SystemType_list[i])



sql_query = f'INSERT INTO tbl_bridge_ibm_segregated_paths_blob_storage('\
    '"UpdatedOn", "UpdatedBy", "CndbId", "DataSource", "Data_source_file_1_path", "Data_source_file_2_path", "Data_source_file_3_path", "Data_source_file_4_path", "Data_source_file_5_path")'\
    'VALUES ({}, {}, {}, {}, {}, {None}, {None}, {None}, {None});'
cursor.execute(sql_query)
conn.commit()
cursor.close()
conn.close()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()

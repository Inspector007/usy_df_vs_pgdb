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

filename = 'C:\project\Server_data_download\server_original\CNDB_ACCOUNT_Records_20210308_1.csv'
filename_2 = 'C:\project\Server_data_download\server_small_df\CNDB_5_columns.csv'


import pandas as pd
cols = ['UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source','SystemType']

data = []
for i in range(25):
    data.append(['2021-03-12','UpdatedBy', 'CndbTags', 'Source','SystemType'])

# df_data = pd.DataFrame(data, columns = cols)
# df_data.to_csv(filename_2)
# import pdb;pdb.set_trace()
table_name = 'tbl_ibm_cndb_staging'
start_time_copy_expert = time.time()
# sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
# cursor.copy_expert(sql, open(filename))
#
# sql = "COPY tbl_ibm_cndb_staging(UpdatedOn,UpdatedBy,CndbTags,Source,SystemType) FROM STDIN DELIMITER \',\' CSV header;"
# cursor.copy_expert(sql, open(filename_2))
with open(filename_2) as f:
    cursor.copy_expert("COPY tbl_ibm_cndb_staging(UpdatedOn,UpdatedBy,CndbTags,Source,SystemType) FROM STDIN WITH HEADER CSV", f)
# with open(filename_2) as f:
#     cursor.copy_from(f, 'tbl_ibm_cndb_staging', columns=('UpdatedOn', 'UpdatedBy', 'CndbTags', 'Source','SystemType'), sep=',')

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()
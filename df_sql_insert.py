import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd
from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
table_name = 'tbl_ibm_file_download_status'
start_time_copy_expert = time.time()
dict_data = {
				'UpdatedOn' : ['2020-03-02',],
				'UpdatedBy' : ['aic_staging',],
				'TenantId' : [1],
				'IsDeleted' : [False,],
				'FileName' : ['aic_staging',],
				'FileCategory' : ['csv',],
				'DownloadStartTime' : ['2020-03-02',],
				'DownloadEndTime' : ['2020-03-02',],
				'StagingProcessStartTIme' : ['2020-03-02',],
				'StagingProcessEndTime' : ['2020-03-02',],
				'DownloadStatus' : ['Processed',],
				'StagingStatus' : ['Failed',],
				'IsMD5Verified' : [True,],
				'IsCRCVerified' : [False,],
				'TotalColumnsInDownLoadedFile' : [3,],
				'TotalRowsInDownLoadedFile' : [4,],
				'RowsInsertedInStaging' : [5,],
				'FileDownloadTags' : ['{"user": {"name": "somename"}}',]
			 }
df_data = pd.DataFrame.from_dict(dict_data)
df_data.to_sql(table_name, con=engine, if_exists='append', index=False)
cursor.close()
connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()
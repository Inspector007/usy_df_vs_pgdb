import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd

db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
db_pass = urllib.parse.quote_plus("Devuser1@3")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required
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
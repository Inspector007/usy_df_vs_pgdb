import pandas as pd
import time , psycopg2
from datetime import datetime
from pgcopy import CopyManager
import urllib.parse
from sqlalchemy import create_engine

# db_user = urllib.parse.quote_plus("")
# db_pass = urllib.parse.quote_plus("")
df_aic_pack = pd.read_csv('C:/project/Server_data_download/CNDB_ACCOUNT_Records_12_Feb_2021_MASKED.csv')
print(df_aic_pack.shape)
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@?sslmode=require', client_encoding='utf8' )
engine = create_engine('postgresql+psycopg2://postgres:portaluser@8877@', client_encoding='utf8' )#, echo=True) ## Dont Echo may take more time
print(engine.table_names()) # list of Tables
connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required
# sql_df = "Select * FROM tbl_ibm_scan_data_aic_package_data_staging"
sql_df = "Select * FROM tbl_user_assignment"
df_fromSQL = pd.read_sql_query(sql_df,connection)
print(df_fromSQL.shape)

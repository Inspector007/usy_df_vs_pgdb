import pandas as pd
import time , psycopg2
from datetime import datetime
from pgcopy import CopyManager
import urllib.parse
from sqlalchemy import create_engine


df_aic_pack = pd.read_csv('C:/project/Server_data_download/CNDB_ACCOUNT_Records_12_Feb_2021_MASKED.csv')
print(df_aic_pack.shape)
from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
# sql_df = "Select * FROM tbl_ibm_scan_data_aic_package_data_staging"
sql_df = "Select * FROM tbl_user_assignment"
df_fromSQL = pd.read_sql_query(sql_df,connection)
print(df_fromSQL.shape)
